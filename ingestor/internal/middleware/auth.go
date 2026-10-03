// Package middleware — auth.go implements API key authentication.
// Keys are stored as SHA-256 hashes, loaded from a mounted Secret.
// The plaintext key is shown to the tenant once at issue time and never
// persisted, so a leak of this file does not leak usable credentials.
package middleware

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var authFailures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ingestor_auth_failures_total",
	Help: "Total authentication failures.",
})

var keyReloads = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ingestor_api_key_reloads_total",
	Help: "API key file reload attempts by result (applied, unchanged, failed).",
}, []string{"result"})

var keysLoaded = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "ingestor_api_keys_loaded",
	Help: "Number of API keys currently accepted.",
})

// APIKeyAuth provides API key based authentication.
//
// Keys are swapped atomically, so a reload never blocks or races an
// in-flight request: each request sees either the old set or the new one.
type APIKeyAuth struct {
	keys             atomic.Pointer[map[string]string] // sha256hex -> tenantID
	logger           *slog.Logger
	path             string
	allowInsecureDev bool

	reloadMu    sync.Mutex // serialises Reload; readers never take it
	fingerprint [sha256.Size]byte
}

// NewAPIKeyAuth loads API keys from a file.
// Format: one line per key — "sha256hex:tenant_id"
// Missing or empty key files fail closed unless local development mode is
// explicitly enabled.
func NewAPIKeyAuth(path string, allowInsecureDev bool, logger *slog.Logger) (*APIKeyAuth, error) {
	a := &APIKeyAuth{logger: logger, path: path, allowInsecureDev: allowInsecureDev}

	keys, err := loadKeys(path)
	if err != nil && allowInsecureDev {
		// Local compose may opt in explicitly; production fails closed.
		logger.Warn("API key file not found; running in dev mode (all requests are tenant 'default')",
			"path", path, "error", err)
		empty := make(map[string]string)
		a.keys.Store(&empty)
		return a, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load API keys: %w (set AUTH_ALLOW_INSECURE_DEV=true only for local development)", err)
	}
	if len(keys) == 0 && !allowInsecureDev {
		return nil, fmt.Errorf("API key file %q is empty", path)
	}

	a.keys.Store(&keys)
	if raw, err := os.ReadFile(path); err == nil {
		a.fingerprint = sha256.Sum256(raw)
	}
	keysLoaded.Set(float64(len(keys)))
	logger.Info("loaded API keys", "count", len(keys), "path", path)
	return a, nil
}

// Reload re-reads the key file and swaps it in if its content changed.
//
// Fail-safe: a missing, unreadable, malformed, or empty file keeps the
// current keys. Revoking every key must be an explicit act (deploy a file
// with a placeholder line), never the side effect of a half-written Secret.
// Returns true when a new key set was applied.
func (a *APIKeyAuth) Reload() (bool, error) {
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	raw, err := os.ReadFile(a.path)
	if err != nil {
		keyReloads.WithLabelValues("failed").Inc()
		return false, fmt.Errorf("read API keys: %w", err)
	}
	fp := sha256.Sum256(raw)
	if bytes.Equal(fp[:], a.fingerprint[:]) {
		keyReloads.WithLabelValues("unchanged").Inc()
		return false, nil
	}
	keys, err := parseKeys(bufio.NewScanner(bytes.NewReader(raw)))
	if err != nil {
		keyReloads.WithLabelValues("failed").Inc()
		return false, fmt.Errorf("parse API keys: %w", err)
	}
	if len(keys) == 0 {
		keyReloads.WithLabelValues("failed").Inc()
		return false, fmt.Errorf("API key file %q is empty; keeping current keys", a.path)
	}

	a.keys.Store(&keys)
	a.fingerprint = fp
	keysLoaded.Set(float64(len(keys)))
	keyReloads.WithLabelValues("applied").Inc()
	a.logger.Info("API keys reloaded", "count", len(keys), "path", a.path)
	return true, nil
}

// Watch polls the key file until ctx is cancelled. Polling by content hash,
// not mtime or inotify, is deliberate: Kubernetes updates Secret volumes by
// swapping a symlink to a new directory, which inotify on the file misses
// and which can leave mtime unchanged.
func (a *APIKeyAuth) Watch(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := a.Reload(); err != nil {
				a.logger.Warn("API key reload failed; keeping current keys", "error", err)
			}
		}
	}
}

func loadKeys(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseKeys(bufio.NewScanner(f))
}

func parseKeys(scanner *bufio.Scanner) (map[string]string, error) {
	keys := make(map[string]string)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid key line (expected hash:tenant): %q", line)
		}
		hash := strings.ToLower(parts[0])
		decoded, err := hex.DecodeString(hash)
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("invalid SHA-256 key hash on line %q", line)
		}
		tenant := parts[1]
		if tenant == "" || len(tenant) > 64 || !validTenantID(tenant) {
			return nil, fmt.Errorf("invalid tenant ID on line %q", line)
		}
		keys[hash] = tenant
	}
	return keys, scanner.Err()
}

func validTenantID(tenant string) bool {
	for _, r := range tenant {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

// Authenticate returns gin middleware that validates the API key.
func (a *APIKeyAuth) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		keysPtr := a.keys.Load()

		// Dev mode: no keys loaded, accept everything as "default"
		if keysPtr == nil || len(*keysPtr) == 0 {
			c.Set(TenantContextKey, DefaultTenantID)
			c.Next()
			return
		}

		scheme, raw, ok := strings.Cut(c.GetHeader("Authorization"), " ")
		raw = strings.TrimSpace(raw)
		if !ok || !strings.EqualFold(scheme, "Bearer") || raw == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing api key"})
			return
		}

		sum := sha256.Sum256([]byte(raw))
		tenant, ok := (*keysPtr)[hex.EncodeToString(sum[:])]
		if !ok {
			authFailures.Inc()
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid api key"})
			return
		}

		c.Set(TenantContextKey, tenant)
		c.Next()
	}
}
