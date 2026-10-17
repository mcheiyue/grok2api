package egress

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
)

const platformCacheTTL = 5 * time.Minute

// ResinAdmin 通过 Resin 管理 API 删除账号的 sticky lease，让传输层失败的
// 账号下一次刷新换出口，而不是永远钉死在坏 exit 上。
type ResinAdmin struct {
	baseURL  string
	token    string
	platform string
	client   *http.Client
	logger   *slog.Logger

	resolvePrefix func(ctx context.Context, nodeID uint64) (string, error)
	platformMu    sync.Mutex
	platformCache map[string]platformCacheEntry
}

type platformCacheEntry struct {
	uuid    string
	expires time.Time
}

// NewResinAdmin 构造管理客户端。baseURL 形如 http://resin:2260（不含尾斜杠），
// platform 为平台 UUID，作为前缀解析失败时的回落值。
func NewResinAdmin(baseURL, token, platform string) *ResinAdmin {
	return &ResinAdmin{
		baseURL:       strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:         strings.TrimSpace(token),
		platform:      strings.TrimSpace(platform),
		client:        &http.Client{Timeout: 3 * time.Second},
		logger:        slog.Default(),
		platformCache: make(map[string]platformCacheEntry),
	}
}

func (a *ResinAdmin) SetLogger(logger *slog.Logger) {
	if logger != nil {
		a.logger = logger
	}
}

// SetNodePlatformResolver 注入「节点 → 账号前缀」解析（manager.PlatformPrefix）。
// 未注入或解析失败时回落到构造时的全局 platform，行为与旧版一致。
func (a *ResinAdmin) SetNodePlatformResolver(fn func(ctx context.Context, nodeID uint64) (string, error)) {
	a.resolvePrefix = fn
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

// ReleaseCredentialSticky 删除该凭据在 Resin 上的 sticky lease（按凭据绑定节点解析平台）。
// 404（无 lease）视为成功，保证幂等。
func (a *ResinAdmin) ReleaseCredentialSticky(ctx context.Context, credential accountdomain.Credential) error {
	return a.ReleaseStickyOnNode(ctx, credential, credential.EgressNodeID)
}

// ReleaseStickyOnNode 按显式节点解析平台并删除 lease。转换路径用实际走过的
// lease.NodeID（可能与凭据绑定节点不同）；404 记日志后仍返回成功——静默 404
// 曾掩盖「删错平台」与「本来没租约」的区别。
func (a *ResinAdmin) ReleaseStickyOnNode(ctx context.Context, credential accountdomain.Credential, nodeID uint64) error {
	identity := stickyIdentity(credential)
	if identity == "" {
		return fmt.Errorf("账号 %d 缺少出口身份", credential.ID)
	}
	platform := a.platformFor(ctx, nodeID)
	endpoint := fmt.Sprintf("%s/api/v1/platforms/%s/leases/%s", a.baseURL, url.PathEscape(platform), url.PathEscape(identity))
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
	if response.StatusCode == http.StatusNotFound {
		a.logger.Warn("sticky_release_missing_lease",
			"account_id", credential.ID,
			"egress_node_id", nodeID,
			"platform", platform,
			"identity", identity,
		)
		return nil
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("resin 删除 lease 返回 HTTP %d", response.StatusCode)
}

// platformFor 用节点账号前缀（US/Grok…）查 Resin 平台 UUID（GET /api/v1/platforms，
// 按名缓存 5 分钟）；前缀缺失、平台未匹配或查询失败均回落全局 platform。
func (a *ResinAdmin) platformFor(ctx context.Context, nodeID uint64) string {
	if a.resolvePrefix == nil || nodeID == 0 {
		return a.platform
	}
	prefix, err := a.resolvePrefix(ctx, nodeID)
	if err != nil {
		a.logger.Warn("sticky_release_platform_prefix_failed", "egress_node_id", nodeID, "error", err)
		return a.platform
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return a.platform
	}
	uuid, ok, err := a.lookupPlatform(ctx, prefix)
	if err != nil {
		a.logger.Warn("sticky_release_platform_lookup_failed", "prefix", prefix, "error", err)
		return a.platform
	}
	if !ok {
		return a.platform
	}
	return uuid
}

type resinPlatform struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (a *ResinAdmin) lookupPlatform(ctx context.Context, name string) (string, bool, error) {
	a.platformMu.Lock()
	if entry, ok := a.platformCache[name]; ok && time.Now().Before(entry.expires) {
		a.platformMu.Unlock()
		return entry.uuid, entry.uuid != "", nil
	}
	a.platformMu.Unlock()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/api/v1/platforms", nil)
	if err != nil {
		return "", false, err
	}
	request.Header.Set("Authorization", "Bearer "+a.token)
	response, err := a.client.Do(request)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", false, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", false, fmt.Errorf("resin 平台列表返回 HTTP %d", response.StatusCode)
	}
	list, err := decodePlatformList(body)
	if err != nil {
		return "", false, err
	}

	uuid := ""
	for _, item := range list {
		if strings.EqualFold(item.Name, name) {
			uuid = item.ID
			break
		}
	}
	a.platformMu.Lock()
	a.platformCache[name] = platformCacheEntry{uuid: uuid, expires: time.Now().Add(platformCacheTTL)}
	a.platformMu.Unlock()
	return uuid, uuid != "", nil
}

func decodePlatformList(body []byte) ([]resinPlatform, error) {
	var list []resinPlatform
	if err := json.Unmarshal(body, &list); err == nil {
		return list, nil
	}
	var wrap struct {
		Data  []resinPlatform `json:"data"`
		Items []resinPlatform `json:"items"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return nil, fmt.Errorf("解析 resine 平台列表: %w", err)
	}
	if len(wrap.Data) > 0 {
		return wrap.Data, nil
	}
	return wrap.Items, nil
}
