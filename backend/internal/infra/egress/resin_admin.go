package egress

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
)

// ResinAdmin 通过 Resin 管理 API 删除账号的 sticky lease，让传输层失败的
// 账号下一次刷新换出口，而不是永远钉死在坏 exit 上。
type ResinAdmin struct {
	baseURL  string
	token    string
	platform string
	client   *http.Client
}

// NewResinAdmin 构造管理客户端。baseURL 形如 http://resin:2260（不含尾斜杠），
// platform 为平台 UUID。
func NewResinAdmin(baseURL, token, platform string) *ResinAdmin {
	return &ResinAdmin{
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:    strings.TrimSpace(token),
		platform: strings.TrimSpace(platform),
		client:   &http.Client{Timeout: 3 * time.Second},
	}
}

// stickyIdentity 复现 WithCredential + normalizeProxyAccount 的账号身份推导，
// 与 SOCKS 用户名里被钉住的 lease 账号保持一致。
func stickyIdentity(credential accountdomain.Credential) string {
	identity := strings.TrimSpace(credential.EgressIdentity)
	if identity == "" {
		provider := credential.Provider
		if provider == "" {
			provider = accountdomain.ProviderBuild
		}
		identity = string(provider) + "_" + fmt.Sprintf("%d", credential.ID)
	}
	return normalizeProxyAccount(identity)
}

// ReleaseCredentialSticky 删除该凭据在 Resin 上的 sticky lease。
// 404（无 lease）视为成功，保证幂等。
func (a *ResinAdmin) ReleaseCredentialSticky(ctx context.Context, credential accountdomain.Credential) error {
	identity := stickyIdentity(credential)
	if identity == "" {
		return fmt.Errorf("账号 %d 缺少出口身份", credential.ID)
	}
	endpoint := fmt.Sprintf("%s/api/v1/platforms/%s/leases/%s", a.baseURL, url.PathEscape(a.platform), url.PathEscape(identity))
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+a.token)
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode == http.StatusNotFound || (response.StatusCode >= 200 && response.StatusCode < 300) {
		return nil
	}
	return fmt.Errorf("resin 删除 lease 返回 HTTP %d", response.StatusCode)
}
