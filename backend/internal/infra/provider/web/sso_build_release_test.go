package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

func TestIsEgressTransportFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"url EOF", &url.Error{Op: "Post", URL: "https://auth.x.ai/oauth2/device/code", Err: io.EOF}, true},
		{"raw EOF", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"ECONNRESET wrapped", &url.Error{Op: "Post", URL: "u", Err: syscall.ECONNRESET}, true},
		{"ECONNREFUSED", syscall.ECONNREFUSED, true},
		{"client cancel", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, true},
		{"http status error", fmt.Errorf("xAI Device Flow 启动失败: %w", conversionHTTPError{status: 502}), false},
		{"unauthorized", provider.ErrUnauthorized, false},
		{"consent business failure", errors.New("SSO 自动批准 Device Flow 失败：未获取 consent_token"), false},
		{"timeout net error", &net.DNSError{IsTimeout: true, Err: "i/o timeout"}, true},
	}
	for _, c := range cases {
		if got := isEgressTransportFailure(c.err); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMarkStickyReleaseDueCooldown(t *testing.T) {
	a := &Adapter{}
	id := uint64(17)
	if !a.markStickyReleaseDue(id) {
		t.Fatal("first release must pass")
	}
	if a.markStickyReleaseDue(id) {
		t.Fatal("second release within cooldown must be blocked")
	}
	if !a.markStickyReleaseDue(id + 1) {
		t.Fatal("different account must not be blocked by another's cooldown")
	}
}

func TestReleaseStickyAfterConversionSkipsWhenNoReleaser(t *testing.T) {
	a := &Adapter{}
	a.releaseStickyAfterConversion(accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2, io.EOF)
	if len(a.stickyReleaseAt) != 0 {
		t.Fatal("nil releaser must not record cooldown state")
	}
}

func TestReleaseStickyAfterConversionSkipsBusinessFailure(t *testing.T) {
	a := &Adapter{}
	stub := &stubStickyReleaser{}
	a.stickyReleaser = stub
	a.releaseStickyAfterConversion(accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}, 2,
		fmt.Errorf("SSO 自动批准 Device Flow 失败: %w", conversionHTTPError{status: 403}))
	time.Sleep(50 * time.Millisecond)
	if stub.calls != 0 {
		t.Fatalf("business failure must not release, got %d calls", stub.calls)
	}
}

type stubStickyReleaser struct {
	calls int
	node  uint64
}

func (s *stubStickyReleaser) ReleaseCredentialSticky(context.Context, accountdomain.Credential) error {
	s.calls++
	return nil
}

func (s *stubStickyReleaser) ReleaseStickyOnNode(_ context.Context, _ accountdomain.Credential, nodeID uint64) error {
	s.calls++
	s.node = nodeID
	return nil
}

func TestReleaseStickyAfterConversionReleasesOnTransportFailure(t *testing.T) {
	a := &Adapter{}
	stub := &stubStickyReleaser{}
	a.stickyReleaser = stub
	cred := accountdomain.Credential{ID: 17, Provider: accountdomain.ProviderWeb}
	a.releaseStickyAfterConversion(cred, 2, &url.Error{Op: "Post", URL: "u", Err: io.EOF})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stub.calls > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stub.calls != 1 {
		t.Fatalf("transport failure must release once, got %d", stub.calls)
	}
	if stub.node != 2 {
		t.Fatalf("released node = %d, want lease node 2", stub.node)
	}
	if a.markStickyReleaseDue(17) {
		t.Fatal("cooldown must be recorded by the release")
	}
}
