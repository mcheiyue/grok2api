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
