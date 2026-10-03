package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func keyLine(raw, tenant string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:]) + ":" + tenant + "\n"
}

func statusFor(a *APIKeyAuth, raw string) int {
	r := gin.New()
	r.Use(a.Authenticate())
	r.GET("/", func(c *gin.Context) { c.String(http.StatusOK, GetTenantID(c)) })
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w.Code
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReload_RotatesKeysWithoutRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys")
	if err := os.WriteFile(path, []byte(keyLine("old-key", "acme")), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := NewAPIKeyAuth(path, false, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if statusFor(a, "old-key") != 200 || statusFor(a, "new-key") != 401 {
		t.Fatal("precondition: only old-key valid")
	}

	_ = os.WriteFile(path, []byte(keyLine("new-key", "acme")), 0600)
	applied, err := a.Reload()
	if err != nil || !applied {
		t.Fatalf("reload: applied=%v err=%v", applied, err)
	}
	if statusFor(a, "new-key") != 200 {
		t.Fatal("rotated key must be accepted after reload")
	}
	if statusFor(a, "old-key") != 401 {
		t.Fatal("revoked key must be refused after reload")
	}
}

func TestReload_UnchangedFileIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys")
	_ = os.WriteFile(path, []byte(keyLine("k", "acme")), 0600)
	a, _ := NewAPIKeyAuth(path, false, quietLogger())
	if applied, err := a.Reload(); applied || err != nil {
		t.Fatalf("unchanged content should not re-apply: applied=%v err=%v", applied, err)
	}
}

func TestReload_BadFileKeepsCurrentKeys(t *testing.T) {
	// A half-written or emptied Secret must never lock every tenant out.
	path := filepath.Join(t.TempDir(), "keys")
	_ = os.WriteFile(path, []byte(keyLine("good", "acme")), 0600)
	a, _ := NewAPIKeyAuth(path, false, quietLogger())

	for name, content := range map[string]string{
		"malformed": "not-a-hash-line\n",
		"empty":     "# all keys commented out\n",
	} {
		_ = os.WriteFile(path, []byte(content), 0600)
		if applied, err := a.Reload(); applied || err == nil {
			t.Fatalf("%s: want error and no apply, got applied=%v err=%v", name, applied, err)
		}
		if statusFor(a, "good") != 200 {
			t.Fatalf("%s: existing key must keep working", name)
		}
	}

	_ = os.Remove(path)
	if _, err := a.Reload(); err == nil {
		t.Fatal("missing file should report an error")
	}
	if statusFor(a, "good") != 200 {
		t.Fatal("missing file must keep current keys")
	}
}

func TestWatch_PicksUpRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys")
	_ = os.WriteFile(path, []byte(keyLine("v1", "acme")), 0600)
	a, _ := NewAPIKeyAuth(path, false, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Watch(ctx, 10*time.Millisecond)

	_ = os.WriteFile(path, []byte(keyLine("v2", "acme")), 0600)
	deadline := time.Now().Add(2 * time.Second)
	for statusFor(a, "v2") != 200 {
		if time.Now().After(deadline) {
			t.Fatal("watcher did not apply the rotated key")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReload_ConcurrentWithRequestsIsRaceFree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys")
	_ = os.WriteFile(path, []byte(keyLine("k0", "acme")), 0600)
	a, _ := NewAPIKeyAuth(path, false, quietLogger())

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			_ = os.WriteFile(path, []byte(keyLine("k0", "acme")+keyLine("extra"+string(rune('a'+i%26)), "acme")), 0600)
			_, _ = a.Reload()
		}
	}()
	for i := 0; i < 500; i++ {
		if statusFor(a, "k0") != 200 {
			t.Fatal("k0 is present in every version of the file and must never be refused")
		}
	}
	<-done
}
