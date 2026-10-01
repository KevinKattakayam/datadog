// Package middleware — auth.go implements API key authentication.
// Keys are stored as SHA-256 hashes, loaded from a mounted Secret.
// The plaintext key is shown to the tenant once at issue time and never
// persisted, so a leak of this file does not leak usable credentials.
package middleware

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var authFailures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ingestor_auth_failures_total",
	Help: "Total authentication failures.",
})

// APIKeyAuth provides API key based authentication.
type APIKeyAuth struct {
	keys   atomic.Pointer[map[string]string] // sha256hex -> tenantID
	logger *slog.Logger
}

// NewAPIKeyAuth loads API keys from a file.
// Format: one line per key — "sha256hex:tenant_id"
// Missing or empty key files fail closed unless local development mode is
// explicitly enabled.
func NewAPIKeyAuth(path string, allowInsecureDev bool, logger *slog.Logger) (*APIKeyAuth, error) {
	a := &APIKeyAuth{logger: logger}

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
	logger.Info("loaded API keys", "count", len(keys), "path", path)
	return a, nil
}

func loadKeys(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	keys := make(map[string]string)
	scanner := bufio.NewScanner(f)
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
