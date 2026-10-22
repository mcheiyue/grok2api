package account

import (
	"context"
	"errors"
	"testing"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

type fakeStickyReleaser struct {
	calls chan accountdomain.Credential
	err   error
}

func (f *fakeStickyReleaser) ReleaseCredentialSticky(_ context.Context, credential accountdomain.Credential) error {
	select {
	case f.calls <- credential:
	default:
	}
	return f.err
}

// 传输层裸错误（oauth_transport_error）+ 显式出口节点 → 必须异步解除 sticky。
func TestRecordCredentialRefreshFailureReleasesStickyOnTransportError(t *testing.T) {
	now := time.Now().UTC()
	service, credential, _ := newCredentialRefreshTestService(t, now)
	releaser := &fakeStickyReleaser{calls: make(chan accountdomain.Credential, 1)}
	service.SetStickyReleaser(releaser)

	credential.EgressNodeID = 16
	service.recordCredentialRefreshFailure(context.Background(), credential, errors.New("connection reset by peer"), false, false)

	select {
	case got := <-releaser.calls:
		if got.ID != credential.ID {
			t.Fatalf("released account %d, want %d", got.ID, credential.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sticky releaser was not called")
	}
}

// 无显式节点（EgressNodeID=0）与非传输类错误码都不触发解除。
func TestRecordCredentialRefreshFailureSkipsStickyRelease(t *testing.T) {
	now := time.Now().UTC()
	service, credential, _ := newCredentialRefreshTestService(t, now)
	releaser := &fakeStickyReleaser{calls: make(chan accountdomain.Credential, 1)}
	service.SetStickyReleaser(releaser)

	credential.EgressNodeID = 0
	service.recordCredentialRefreshFailure(context.Background(), credential, errors.New("connection reset by peer"), false, false)

	credential.EgressNodeID = 16
	typed := &provider.CredentialRefreshError{Status: 401, Code: "invalid_grant", Message: "grant gone", Permanent: true}
	service.recordCredentialRefreshFailure(context.Background(), credential, typed, false, false)

	select {
	case got := <-releaser.calls:
		t.Fatalf("unexpected sticky release for account %d", got.ID)
	case <-time.After(200 * time.Millisecond):
	}
}

// 未注入 releaser 时触发路径必须安静（不 panic）。
func TestRecordCredentialRefreshFailureWithoutReleaser(t *testing.T) {
	now := time.Now().UTC()
	service, credential, _ := newCredentialRefreshTestService(t, now)
	credential.EgressNodeID = 16
	service.recordCredentialRefreshFailure(context.Background(), credential, errors.New("connection reset by peer"), false, false)
}

// 连接级失败必须释放 sticky 后立刻换节点重试一次，而不是等 1h 退避后撞回同一节点。
func TestEnsureCredentialRetriesOnceWithNewExitOnTransportError(t *testing.T) {
	now := time.Now().UTC()
	service, credential, adapter := newCredentialRefreshTestService(t, now)
	releaser := &fakeStickyReleaser{calls: make(chan accountdomain.Credential, 2)}
	service.SetStickyReleaser(releaser)

	credential.EgressNodeID = 16
	adapter.errs = []error{errors.New(`Post "https://auth.x.ai/oauth2/token": EOF`)}

	refreshed, err := service.ensureCredential(context.Background(), credential, ensureCredentialOptions{force: true, bypassCooldown: true})
	if err != nil {
		t.Fatalf("refresh after new exit failed: %v", err)
	}
	if got := adapter.refreshCount.Load(); got != 2 {
		t.Fatalf("refresh attempts = %d, want 2", got)
	}
	if refreshed.EncryptedAccessToken != "access-2" {
		t.Fatalf("token = %q, want access-2", refreshed.EncryptedAccessToken)
	}
	select {
	case got := <-releaser.calls:
		if got.ID != credential.ID {
			t.Fatalf("released account %d, want %d", got.ID, credential.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sticky releaser was not called before retry")
	}
}

// 上游已给出 HTTP 状态的失败不换出口重试：换节点无用且会加大上游压力。
func TestEnsureCredentialDoesNotRetryOnHTTPError(t *testing.T) {
	now := time.Now().UTC()
	service, credential, adapter := newCredentialRefreshTestService(t, now)
	releaser := &fakeStickyReleaser{calls: make(chan accountdomain.Credential, 2)}
	service.SetStickyReleaser(releaser)

	credential.EgressNodeID = 16
	typed := &provider.CredentialRefreshError{Status: 429, Code: "oauth_http_error", Message: "rate limited"}
	adapter.errs = []error{typed, typed}

	if _, err := service.ensureCredential(context.Background(), credential, ensureCredentialOptions{force: true, bypassCooldown: true}); err == nil {
		t.Fatal("expected refresh failure")
	}
	if got := adapter.refreshCount.Load(); got != 1 {
		t.Fatalf("refresh attempts = %d, want 1", got)
	}
}
