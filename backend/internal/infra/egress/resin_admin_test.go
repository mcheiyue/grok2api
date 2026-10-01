package egress

import (
	"context"
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
