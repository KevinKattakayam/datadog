package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAPIKeyAuth_ValidKey(t *testing.T) {
	key := "secret-token-12345"
	sum := sha256.Sum256([]byte(key))
	hashHex := hex.EncodeToString(sum[:])

	tmpDir := t.TempDir()
	keyFile := filepath.Join(tmpDir, "keys.txt")
	err := os.WriteFile(keyFile, []byte(fmt.Sprintf("%s:tenant-alpha\n", hashHex)), 0600)
	if err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	auth, err := NewAPIKeyAuth(keyFile, false, slog.Default())
	if err != nil {
		t.Fatalf("failed to create auth: %v", err)
	}

	r := gin.New()
	r.Use(auth.Authenticate())
	r.GET("/protected", func(c *gin.Context) {
		c.String(http.StatusOK, GetTenantID(c))
	})

	req := httptest.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "tenant-alpha" {
		t.Errorf("expected 'tenant-alpha', got %s", w.Body.String())
	}
}

func TestAPIKeyAuth_MissingAndInvalidKey(t *testing.T) {
	key := "valid-token"
	sum := sha256.Sum256([]byte(key))
	hashHex := hex.EncodeToString(sum[:])

	tmpDir := t.TempDir()
	keyFile := filepath.Join(tmpDir, "keys.txt")
	_ = os.WriteFile(keyFile, []byte(fmt.Sprintf("%s:tenant-alpha\n", hashHex)), 0600)

	auth, err := NewAPIKeyAuth(keyFile, false, slog.Default())
	if err != nil {
		t.Fatalf("failed to create auth: %v", err)
	}

	r := gin.New()
	r.Use(auth.Authenticate())
	r.GET("/protected", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Missing header
	req := httptest.NewRequest("GET", "/protected", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing key, got %d", w.Code)
	}

	// Invalid key
	req = httptest.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer invalid-key")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for invalid key, got %d", w.Code)
	}
}

func TestAPIKeyAuth_FailsClosedWithoutKeyFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-keys")
	if _, err := NewAPIKeyAuth(missing, false, slog.Default()); err == nil {
		t.Fatal("expected missing key file to fail closed")
	}

	auth, err := NewAPIKeyAuth(missing, true, slog.Default())
	if err != nil {
		t.Fatalf("explicit development mode should allow a missing key file: %v", err)
	}
	if got := auth.keys.Load(); got == nil || len(*got) != 0 {
		t.Fatal("development mode should initialize an empty key map")
	}
}

func TestLoadKeysRejectsMalformedCredentials(t *testing.T) {
	for name, contents := range map[string]string{
		"short hash":    "abcd:tenant-a\n",
		"empty tenant":  fmt.Sprintf("%064x:\n", 1),
		"unsafe tenant": fmt.Sprintf("%064x:tenant/other\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "keys.txt")
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadKeys(path); err == nil {
				t.Fatal("expected malformed key file to be rejected")
			}
		})
	}
}
