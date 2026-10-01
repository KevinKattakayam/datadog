package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestTenantExtractor_HeaderPresent(t *testing.T) {
	r := gin.New()
	r.Use(TenantExtractor())
	r.GET("/test", func(c *gin.Context) {
		tid := GetTenantID(c)
		c.String(http.StatusOK, tid)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "Acme-Corp")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Body.String() != "acme-corp" {
		t.Errorf("expected tenant 'acme-corp', got '%s'", w.Body.String())
	}
}

func TestTenantExtractor_NoHeader(t *testing.T) {
	r := gin.New()
	r.Use(TenantExtractor())
	r.GET("/test", func(c *gin.Context) {
		tid := GetTenantID(c)
		c.String(http.StatusOK, tid)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Body.String() != DefaultTenantID {
		t.Errorf("expected default tenant '%s', got '%s'", DefaultTenantID, w.Body.String())
	}
}

func TestTenantRateLimit_Allows(t *testing.T) {
	limiter := NewTenantRateLimiter(100, nil)

	r := gin.New()
	r.Use(TenantExtractor())
	r.Use(TenantRateLimit(limiter))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "test-tenant")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestTenantRateLimit_Blocks(t *testing.T) {
	// Create limiter with 1 RPS
	limiter := NewTenantRateLimiter(1, nil)

	r := gin.New()
	r.Use(TenantExtractor())
	r.Use(TenantRateLimit(limiter))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// First request should pass
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "burst-tenant")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("first request: expected 200, got %d", w.Code)
	}

	// Second request (immediate) should be rate limited
	req = httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "burst-tenant")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("second request: expected 429, got %d", w.Code)
	}
}

func TestTenantRateLimit_DisabledTenant(t *testing.T) {
	configs := map[string]TenantConfig{
		"disabled-tenant": {MaxRPS: 100, Enabled: false},
	}
	limiter := NewTenantRateLimiter(100, configs)

	r := gin.New()
	r.Use(TenantExtractor())
	r.Use(TenantRateLimit(limiter))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "disabled-tenant")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 for disabled tenant, got %d", w.Code)
	}
}

func TestTenantRateLimit_PerTenantIsolation(t *testing.T) {
	// 1 RPS per tenant
	limiter := NewTenantRateLimiter(1, nil)

	r := gin.New()
	r.Use(TenantExtractor())
	r.Use(TenantRateLimit(limiter))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Tenant A — first request passes
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "tenant-a")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("tenant-a first: expected 200, got %d", w.Code)
	}

	// Tenant B — should still pass (independent bucket)
	req = httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TenantHeaderKey, "tenant-b")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("tenant-b first: expected 200, got %d", w.Code)
	}
}

func TestTenantRateLimit_ConcurrentRace(t *testing.T) {
	// 1000 concurrent goroutines testing race condition under go test -race
	limiter := NewTenantRateLimiter(500, nil)

	r := gin.New()
	r.Use(TenantExtractor())
	r.Use(TenantRateLimit(limiter))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	done := make(chan bool)
	for i := 0; i < 1000; i++ {
		go func() {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set(TenantHeaderKey, "concurrent-tenant")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			done <- true
		}()
	}

	for i := 0; i < 1000; i++ {
		<-done
	}
}
