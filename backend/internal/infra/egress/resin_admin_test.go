package egress

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
)

func TestStickyIdentityMirrorsProxyRender(t *testing.T) {
	explicit := stickyIdentity(accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild, EgressIdentity: "My Acct"})
	if explicit != "My_Acct" {
		t.Fatalf("explicit identity not normalized: %q", explicit)
	}
	derived := stickyIdentity(accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild})
	if derived != "grok_build_58" {
		t.Fatalf("derived identity = %q", derived)
	}
	empty := stickyIdentity(accountdomain.Credential{ID: 7})
	if empty != "grok_build_7" {
		t.Fatalf("provider fallback identity = %q", empty)
	}
}

func TestReleaseCredentialStickyDeletesLease(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotAuth = r.Method, r.URL.EscapedPath(), r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "plat-1")
	err := admin.ReleaseCredentialSticky(context.Background(), accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild})
	if err != nil {
		t.Fatalf("release failed: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %s", gotMethod)
	}
	if gotPath != "/api/v1/platforms/plat-1/leases/grok_build_58" {
		t.Fatalf("path = %s", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
}

func TestReleaseCredentialStickyTreatsNotFoundAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "plat-1")
	if err := admin.ReleaseCredentialSticky(context.Background(), accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild}); err != nil {
		t.Fatalf("404 should be idempotent success: %v", err)
	}
}

func TestReleaseCredentialStickyReportsServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "plat-1")
	if err := admin.ReleaseCredentialSticky(context.Background(), accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild}); err == nil {
		t.Fatal("HTTP 500 accepted as success")
	}
}

func TestReleaseCredentialStickyReportsTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.Close() // 立即关闭，制造连接失败
	admin := NewResinAdmin(server.URL, "tok", "plat-1")
	if err := admin.ReleaseCredentialSticky(context.Background(), accountdomain.Credential{ID: 58, Provider: accountdomain.ProviderBuild}); err == nil {
		t.Fatal("transport failure accepted as success")
	}
}

func TestProxyPlatformPrefix(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"socks5h://US.{account}@resin:2260", "US"},
		{"socks5h://Grok.{account}:pw@resin:2260", "Grok"},
		{"http://1.2.3.4:8080", ""},
		{"socks5h://127.0.0.1:1080", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := proxyPlatformPrefix(c.raw); got != c.want {
			t.Fatalf("proxyPlatformPrefix(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}

func TestReleaseStickyOnNodeResolvesPlatformByPrefix(t *testing.T) {
	var gotDeletePath string
	gotPlatformsHits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/platforms":
			gotPlatformsHits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"uuid-us","name":"US"},{"id":"uuid-grok","name":"Grok"}]`))
		case r.Method == http.MethodDelete:
			gotDeletePath = r.URL.EscapedPath()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "uuid-grok")
	admin.SetNodePlatformResolver(func(context.Context, uint64) (string, error) { return "US", nil })

	err := admin.ReleaseStickyOnNode(context.Background(), accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2)
	if err != nil {
		t.Fatalf("release failed: %v", err)
	}
	if gotDeletePath != "/api/v1/platforms/uuid-us/leases/grok_web_17" {
		t.Fatalf("delete path = %s, want platform resolved to uuid-us", gotDeletePath)
	}
	if gotPlatformsHits != 1 {
		t.Fatalf("platform list hits = %d, want 1", gotPlatformsHits)
	}
}

func TestReleaseStickyFallsBackToConfiguredPlatformOnResolverFailure(t *testing.T) {
	var gotDeletePath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			gotDeletePath = r.URL.EscapedPath()
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "uuid-grok")
	admin.SetNodePlatformResolver(func(context.Context, uint64) (string, error) { return "", errors.New("boom") })

	if err := admin.ReleaseStickyOnNode(context.Background(), accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2); err != nil {
		t.Fatalf("release failed: %v", err)
	}
	if gotDeletePath != "/api/v1/platforms/uuid-grok/leases/grok_web_17" {
		t.Fatalf("delete path = %s, want fallback to configured uuid-grok", gotDeletePath)
	}
}

func TestReleaseStickyFallsBackWhenPrefixMatchesNoPlatform(t *testing.T) {
	var gotDeletePath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/platforms":
			_, _ = w.Write([]byte(`[{"id":"uuid-grok","name":"Grok"}]`))
		case r.Method == http.MethodDelete:
			gotDeletePath = r.URL.EscapedPath()
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "uuid-grok")
	admin.SetNodePlatformResolver(func(context.Context, uint64) (string, error) { return "US", nil })

	if err := admin.ReleaseStickyOnNode(context.Background(), accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2); err != nil {
		t.Fatalf("release failed: %v", err)
	}
	if gotDeletePath != "/api/v1/platforms/uuid-grok/leases/grok_web_17" {
		t.Fatalf("delete path = %s, want fallback to configured uuid-grok", gotDeletePath)
	}
}

func TestReleaseCredentialStickyStillTreatsNotFoundAsSuccessWithResolver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`[{"id":"uuid-us","name":"US"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	admin := NewResinAdmin(server.URL, "tok", "uuid-grok")
	admin.SetNodePlatformResolver(func(context.Context, uint64) (string, error) { return "US", nil })
	if err := admin.ReleaseStickyOnNode(context.Background(), accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2); err != nil {
		t.Fatalf("404 must stay idempotent success: %v", err)
	}
}
