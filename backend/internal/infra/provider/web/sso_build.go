package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	egressdomain "github.com/chenyme/grok2api/backend/internal/domain/egress"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
	"golang.org/x/net/html"
)

const (
	ssoBuildClientID = "b1a00492-073a-47ea-816f-4c329264a828"
	ssoBuildScope    = "openid profile email offline_access grok-cli:access api:access"
	ssoAuthorizeURL  = "https://auth.x.ai/oauth2/authorize"
	ssoDeviceURL     = "https://auth.x.ai/oauth2/device/code"
	ssoVerifyURL     = "https://auth.x.ai/oauth2/device/verify"
	ssoApproveURL    = "https://auth.x.ai/oauth2/device/approve"
	ssoTokenURL      = "https://auth.x.ai/oauth2/token"
	ssoRedirectURI   = "http://127.0.0.1:56121/callback"
	ssoGrokReferrer  = "grok-build"
	ssoGrokPlan      = "generic"
	ssoNextActionID  = "401ce882800a9c98c11b0066c135231259bc3b003a"
	ssoGrokTokenUA   = "grok-pager/0.2.93 grok-shell/0.2.93 (linux; x86_64)"
	maxAuthBody      = 2 << 20
	consentAudience  = "consent"
)

type ssoBuildHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type ssoBuildFlow struct {
	client    ssoBuildHTTPClient
	userAgent string
	cookies   map[string]string
	logger    *slog.Logger
}

func (f *ssoBuildFlow) log() *slog.Logger {
	if f.logger != nil {
		return f.logger
	}
	return slog.Default()
}

// stickyReleaser 与 account.StickyReleaser 同形，web 侧不反向依赖 application 层。
type stickyReleaser interface {
	ReleaseCredentialSticky(ctx context.Context, credential accountdomain.Credential) error
	ReleaseStickyOnNode(ctx context.Context, credential accountdomain.Credential, nodeID uint64) error
}

const (
	stickyReleaseCooldown = 10 * time.Minute
	stickyReleaseTimeout  = 10 * time.Second
)

func (a *Adapter) ConvertToBuild(ctx context.Context, credential accountdomain.Credential) (provider.CredentialSeed, error) {
	if credential.Provider != accountdomain.ProviderWeb || credential.AuthType != accountdomain.AuthTypeSSO {
		return provider.CredentialSeed{}, fmt.Errorf("仅 Grok Web SSO 账号支持转换")
	}
	token, err := a.cipher.Decrypt(credential.EncryptedAccessToken)
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("解密 Grok Web SSO: %w", err)
	}
	token = normalizeSSOToken(token)
	if token == "" {
		return provider.CredentialSeed{}, provider.ErrUnauthorized
	}
	lease, err := a.egress.AcquireCredential(ctx, egressdomain.ScopeWeb, credential)
	if err != nil {
		return provider.CredentialSeed{}, err
	}
	defer lease.Release()
	requestCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	flow := &ssoBuildFlow{
		client: lease, userAgent: lease.UserAgent,
		cookies: ssoBuildCookies(token, lease.CFCookies),
		logger:  a.log(),
	}
	seed, err := flow.convert(requestCtx, credential)
	if err != nil {
		a.egress.Feedback(context.WithoutCancel(ctx), lease.NodeID, conversionStatus(err), err)
		a.releaseStickyAfterConversion(credential, lease.NodeID, err)
		return provider.CredentialSeed{}, err
	}
	a.egress.Feedback(context.WithoutCancel(ctx), lease.NodeID, http.StatusOK, nil)
	return seed, nil
}

// releaseStickyAfterConversion 只对出口类传输失败异步解除租约（L1 自愈触发点）；
// 业务失败（401/consent/解析）不触发，每账号 10 分钟冷却防打爆 Resin admin，
// 任何失败仅记日志、不阻断转换主流程。
func (a *Adapter) releaseStickyAfterConversion(credential accountdomain.Credential, nodeID uint64, err error) {
	releaser := a.stickyReleaser
	if releaser == nil || nodeID == 0 || !isEgressTransportFailure(err) {
		return
	}
	if !a.markStickyReleaseDue(credential.ID) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), stickyReleaseTimeout)
		defer cancel()
		if releaseErr := releaser.ReleaseStickyOnNode(ctx, credential, nodeID); releaseErr != nil {
			a.log().Warn("sticky_release_failed",
				"account_id", credential.ID,
				"egress_node_id", nodeID,
				"error", releaseErr,
			)
			return
		}
		a.log().Info("sticky_released",
			"account_id", credential.ID,
			"egress_node_id", nodeID,
		)
	}()
}

// markStickyReleaseDue 记录每账号释放时刻并在冷却窗口内放行；惰性清理
// 过期条目，条目量受转换频率约束（7k 账号 × 10 分钟窗口）。
func (a *Adapter) markStickyReleaseDue(accountID uint64) bool {
	now := time.Now()
	a.stickyMu.Lock()
	defer a.stickyMu.Unlock()
	if a.stickyReleaseAt == nil {
		a.stickyReleaseAt = make(map[uint64]time.Time)
	}
	for id, at := range a.stickyReleaseAt {
		if now.Sub(at) >= stickyReleaseCooldown {
			delete(a.stickyReleaseAt, id)
		}
	}
	if at, ok := a.stickyReleaseAt[accountID]; ok && now.Sub(at) < stickyReleaseCooldown {
		return false
	}
	a.stickyReleaseAt[accountID] = now
	return true
}

// isEgressTransportFailure 判定转换失败是否为出口类传输错误（决定是否换租约）。
// EOF/reset/超时/dial 失败 = 出口问题；HTTP 状态错误、业务拒绝（consent/解析/401）
// 与客户端主动取消不触发释放。
func isEgressTransportFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var statusErr conversionHTTPError
	if errors.As(err, &statusErr) {
		return false
	}
	if errors.Is(err, provider.ErrUnauthorized) || errors.Is(err, provider.ErrAuthorizationDenied) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

func (f *ssoBuildFlow) convert(ctx context.Context, credential accountdomain.Credential) (provider.CredentialSeed, error) {
	seed, err := f.convertWithAuthCode(ctx, credential)
	if err == nil {
		return seed, nil
	}
	f.log().Warn("sso_auth_code_fallback_to_device",
		"account_id", credential.ID,
		"auth_code_error", err,
	)
	return f.convertWithDeviceFlow(ctx, credential)
}

func (f *ssoBuildFlow) convertWithDeviceFlow(ctx context.Context, credential accountdomain.Credential) (provider.CredentialSeed, error) {
	form := url.Values{"client_id": {ssoBuildClientID}, "scope": {ssoBuildScope}}
	status, _, body, err := f.do(ctx, http.MethodPost, ssoDeviceURL, form)
	if err != nil {
		return provider.CredentialSeed{}, err
	}
	if status < 200 || status >= 300 {
		return provider.CredentialSeed{}, fmt.Errorf("xAI Device Flow 启动失败: %w", conversionHTTPError{status: status})
	}
	var device struct {
		DeviceCode string `json:"device_code"`
		UserCode   string `json:"user_code"`
		Interval   int    `json:"interval"`
		ExpiresIn  int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &device); err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("解析 xAI Device Flow: %w", err)
	}
	if device.DeviceCode == "" || device.UserCode == "" {
		return provider.CredentialSeed{}, fmt.Errorf("xAI Device Flow 返回字段不完整")
	}
	if device.Interval <= 0 {
		device.Interval = 5
	}
	if device.ExpiresIn <= 0 {
		device.ExpiresIn = 1800
	}

	// verify → consent。xAI 2025-09 起在 device 授权流程加入防伪令牌：consent 页面内嵌一个
	// 一次性 consent_token（ES256 JWT，typ=consent+jwt），approve 必须回传它，否则返回
	// 403「Request could not be verified」。因此这里不能只看 3xx 的状态路径，还要取到
	// consent 页面来解析该令牌。
	status, finalURL, verifyBody, err := f.doWithFollow(ctx, http.MethodPost, ssoVerifyURL, url.Values{"user_code": {device.UserCode}}, false)
	if err != nil {
		return provider.CredentialSeed{}, err
	}
	if status == http.StatusUnauthorized {
		return provider.CredentialSeed{}, provider.ErrUnauthorized
	}
	if status < 200 || status >= 400 {
		return provider.CredentialSeed{}, fmt.Errorf("SSO 自动验证 Device Flow 失败: %w", conversionHTTPError{status: status})
	}
	if redirectState := ssoDeviceRedirectState(finalURL); redirectState != "consent" {
		if redirectState == "sign-in" {
			return provider.CredentialSeed{}, provider.ErrUnauthorized
		}
		return provider.CredentialSeed{}, fmt.Errorf("SSO 自动验证 Device Flow 失败")
	}
	consentPageURL := finalURL

	// verify 以 3xx 跳转到 consent 时响应体为空，需要 GET consent 页面本身来解析 consent_token。
	consentToken := extractConsentToken(verifyBody)
	consentUserCode := extractConsentInput(verifyBody, "user_code")
	if consentToken == "" {
		consentStatus, _, consentBody, cerr := f.do(ctx, http.MethodGet, consentPageURL, nil)
		if cerr != nil {
			return provider.CredentialSeed{}, fmt.Errorf("获取 xAI consent 页面失败: %w", cerr)
		}
		if consentStatus < 200 || consentStatus >= 300 {
			return provider.CredentialSeed{}, fmt.Errorf("获取 xAI consent 页面失败: %w", conversionHTTPError{status: consentStatus})
		}
		consentToken = extractConsentToken(consentBody)
		consentUserCode = extractConsentInput(consentBody, "user_code")
	}
	// consent_token 仅在通过校验时回传；缺失或不可用时不阻断，按面板路径继续 approve
	// （面板从不校验也不回传该令牌，实测 48/48 成功）。
	approveForm := url.Values{
		"user_code": {device.UserCode}, "action": {"allow"}, "principal_type": {"User"}, "principal_id": {""},
	}
	if consentTokenValid(consentToken, credential.UserID) || consentTokenMatchesUserCode(consentToken, consentUserCode, device.UserCode, time.Now()) {
		approveForm.Set("consent_token", consentToken)
	} else {
		f.log().Warn("sso_consent_token_unusable",
			"account_id", credential.ID,
			"token_present", consentToken != "",
		)
	}

	var approveBody []byte
	status, finalURL, approveBody, err = f.doWithFollowHeaders(ctx, http.MethodPost, ssoApproveURL, approveForm, false, map[string]string{
		"Origin":                    "https://accounts.x.ai",
		"Referer":                   consentPageURL,
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "same-site",
		"Sec-Fetch-User":            "?1",
		"Upgrade-Insecure-Requests": "1",
	})
	if err != nil {
		return provider.CredentialSeed{}, err
	}
	if status == http.StatusForbidden && isBrowserVerificationRequired(approveBody) {
		// Observed 2026-10-20: approve now demands a Castle browser token that only
		// the consent page's script can produce. Not an SSO failure.
		return provider.CredentialSeed{}, fmt.Errorf("%w: xAI 要求浏览器校验后才能批准授权，请改用 Device OAuth 在浏览器中手动完成", errBrowserVerificationRequired)
	}
	if status < 200 || status >= 400 {
		return provider.CredentialSeed{}, fmt.Errorf("SSO 自动批准 Device Flow 失败: %w", conversionHTTPError{status: status})
	}
	if redirectState := ssoDeviceRedirectState(finalURL); redirectState != "done" {
		if redirectState == "sign-in" {
			return provider.CredentialSeed{}, provider.ErrUnauthorized
		}
		return provider.CredentialSeed{}, fmt.Errorf("SSO 自动批准 Device Flow 失败")
	}

	token, err := f.pollToken(ctx, device.DeviceCode, time.Duration(device.Interval)*time.Second, time.Duration(device.ExpiresIn)*time.Second)
	if err != nil {
		return provider.CredentialSeed{}, err
	}
	return f.buildCredentialSeed(credential, token)
}

func (f *ssoBuildFlow) convertWithAuthCode(ctx context.Context, credential accountdomain.Credential) (provider.CredentialSeed, error) {
	verifier, challenge, err := generatePKCE()
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("生成 PKCE 密钥: %w", err)
	}
	state, err := randomHex(16)
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("生成 OAuth state: %w", err)
	}
	nonce, err := randomHex(16)
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("生成 OAuth nonce: %w", err)
	}

	params := url.Values{
		"client_id":             {ssoBuildClientID},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
		"nonce":                 {nonce},
		"plan":                  {ssoGrokPlan},
		"redirect_uri":          {ssoRedirectURI},
		"referrer":              {ssoGrokReferrer},
		"response_type":         {"code"},
		"scope":                 {ssoBuildScope},
		"state":                 {state},
	}
	authorizeURL := ssoAuthorizeURL + "?" + params.Encode()

	status, finalURL, body, err := f.doWithFollow(ctx, http.MethodGet, authorizeURL, nil, true)
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("打开 authorize 页面失败: %w", err)
	}
	if status == http.StatusUnauthorized || strings.Contains(finalURL, "sign-in") || strings.Contains(finalURL, "sign-up") {
		return provider.CredentialSeed{}, provider.ErrUnauthorized
	}
	if !strings.Contains(finalURL, "/oauth2/consent") {
		return provider.CredentialSeed{}, fmt.Errorf("authorize 未进入 consent 流程: url=%s status=%d", finalURL, status)
	}

	actionIDs := extractNextActionIDs(string(body))
	if len(actionIDs) == 0 {
		actionIDs = []string{ssoNextActionID}
	}

	consentPayload, err := json.Marshal([]map[string]any{{
		"action":              "allow",
		"clientId":            ssoBuildClientID,
		"redirectUri":         ssoRedirectURI,
		"scope":               ssoBuildScope,
		"state":               state,
		"codeChallenge":       challenge,
		"codeChallengeMethod": "S256",
		"nonce":               nonce,
		"principalType":       "User",
		"principalId":         "",
		"referrer":            ssoGrokReferrer,
	}})
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("编码 consent payload: %w", err)
	}

	var authCode string
	for _, actionID := range actionIDs {
		status, _, respBody, postErr := f.doRawPost(ctx, finalURL, consentPayload, map[string]string{
			"Content-Type": "text/plain;charset=UTF-8",
			"Accept":       "text/x-component",
			"Origin":       "https://accounts.x.ai",
			"Referer":      finalURL,
			"Next-Action":  actionID,
		})
		if postErr != nil {
			f.log().Warn("sso_next_action_request_failed", "action_id", actionID, "error", postErr)
			continue
		}
		if status < 200 || status >= 300 {
			f.log().Warn("sso_next_action_http_error", "action_id", actionID, "status", status)
			continue
		}
		code, parseErr := parseConsentResult(string(respBody))
		if parseErr != "" {
			f.log().Warn("sso_consent_server_rejected", "action_id", actionID, "error", parseErr)
			return provider.CredentialSeed{}, fmt.Errorf("consent 被服务端拒绝: %s", parseErr)
		}
		if code != "" {
			authCode = code
			break
		}
	}
	if authCode == "" {
		return provider.CredentialSeed{}, fmt.Errorf("所有 Next-Action 均未返回 authorization code")
	}

	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {authCode},
		"redirect_uri":  {ssoRedirectURI},
		"client_id":     {ssoBuildClientID},
		"code_verifier": {verifier},
	}
	status, _, tokenBody, err := f.doWithFollowHeaders(ctx, http.MethodPost, ssoTokenURL, tokenForm, false, map[string]string{
		"User-Agent":            ssoGrokTokenUA,
		"X-Grok-Client-Version": "0.2.93",
		"Accept":                "*/*",
	})
	if err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("Auth Code 换取 Token 传输错误: %w", err)
	}
	if status < 200 || status >= 300 {
		return provider.CredentialSeed{}, fmt.Errorf("Auth Code 换取 Token 失败 (HTTP %d): %s", status, string(tokenBody))
	}

	var tokenPayload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		ExpiresIn    int    `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}
	if err := json.Unmarshal(tokenBody, &tokenPayload); err != nil {
		return provider.CredentialSeed{}, fmt.Errorf("解析 OAuth Token 响应: %w", err)
	}
	if tokenPayload.AccessToken == "" {
		return provider.CredentialSeed{}, fmt.Errorf("OAuth Token 缺少 access_token")
	}
	if tokenPayload.ExpiresIn <= 0 {
		tokenPayload.ExpiresIn = 21600
	}

	token := ssoBuildToken{
		AccessToken:  tokenPayload.AccessToken,
		RefreshToken: tokenPayload.RefreshToken,
		IDToken:      tokenPayload.IDToken,
		ExpiresAt:    time.Now().UTC().Add(time.Duration(tokenPayload.ExpiresIn) * time.Second),
	}
	return f.buildCredentialSeed(credential, token)
}

func (f *ssoBuildFlow) doRawPost(ctx context.Context, endpoint string, payload []byte, headers map[string]string) (int, string, []byte, error) {
	if !safeXAIURL(endpoint) {
		return 0, "", nil, fmt.Errorf("xAI OAuth URL 不安全")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return 0, "", nil, err
	}
	request.Header.Set("User-Agent", f.userAgent)
	request.Header.Set("Cookie", f.cookieHeader())
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response, err := f.client.Do(request)
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = response.Body.Close() }()
	f.captureCookies(response)
	data, readErr := io.ReadAll(io.LimitReader(response.Body, maxAuthBody+1))
	if readErr != nil {
		return response.StatusCode, endpoint, nil, readErr
	}
	return response.StatusCode, endpoint, data, nil
}

func generatePKCE() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	verifier := base64.RawURLEncoding.EncodeToString(raw)
	h := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(h[:])
	return verifier, challenge, nil
}

func randomHex(bytesLen int) (string, error) {
	b := make([]byte, bytesLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

var (
	nextActionRegexes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:\$ACTION_ID_|next-action["']?\s*[:=]\s*["']|["'])([0-9a-f]{40,44})["']`),
		regexp.MustCompile(`(?i)createServerReference\)?\(["']([0-9a-f]{40,44})["']`),
		regexp.MustCompile(`(?i)["']([0-9a-f]{40,44})["']\s*,\s*(?:callServer|findSourceMapURL)`),
	}
)

func extractNextActionIDs(htmlContent string) []string {
	var list []string
	seen := make(map[string]bool)
	add := func(id string) {
		id = strings.ToLower(strings.TrimSpace(id))
		if len(id) >= 40 && len(id) <= 44 && !seen[id] {
			seen[id] = true
			list = append(list, id)
		}
	}
	add(ssoNextActionID)
	for _, re := range nextActionRegexes {
		for _, m := range re.FindAllStringSubmatch(htmlContent, -1) {
			if len(m) > 1 {
				add(m[1])
			}
		}
	}
	return list
}

func parseConsentResult(responseBody string) (string, string) {
	var errText string
	for _, line := range strings.Split(responseBody, "\n") {
		idx := strings.Index(line, "{")
		if idx < 0 {
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(line[idx:]), &parsed); err != nil {
			continue
		}
		if code, ok := parsed["code"].(string); ok && code != "" && parsed["success"] != false {
			return code, ""
		}
		if errMsg, ok := parsed["error"].(string); ok && errMsg != "" {
			errText = errMsg
		} else if success, ok := parsed["success"].(bool); ok && !success && errText == "" {
			errText = "success=false"
		}
	}
	return "", errText
}

func (f *ssoBuildFlow) buildCredentialSeed(credential accountdomain.Credential, token ssoBuildToken) (provider.CredentialSeed, error) {
	claims := decodeBuildClaims(firstValue(token.IDToken, token.AccessToken))
	userID := claimString(claims, "sub")
	email := claimString(claims, "email")
	teamID := claimString(claims, "team_id")
	name := strings.TrimSpace(credential.Name)
	if name == "" {
		name = "Grok Web account"
	}
	return provider.CredentialSeed{
		Provider: accountdomain.ProviderBuild, AuthType: accountdomain.AuthTypeOAuth,
		Name: firstValue(email, name+" Build", userID, "Grok Build account"), Email: email, UserID: userID, TeamID: teamID,
		SourceKey: "sso-build:" + security.HashToken(token.AccessToken), OIDCClientID: ssoBuildClientID,
		AccessToken: token.AccessToken, RefreshToken: token.RefreshToken, ExpiresAt: token.ExpiresAt,
	}, nil
}

type ssoBuildToken struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	ExpiresAt    time.Time
}

func (f *ssoBuildFlow) pollToken(ctx context.Context, deviceCode string, interval, expiresIn time.Duration) (ssoBuildToken, error) {
	if interval < time.Second {
		interval = time.Second
	}
	deadline := time.Now().Add(min(expiresIn, 75*time.Second))
	for time.Now().Before(deadline) {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ssoBuildToken{}, ctx.Err()
		case <-timer.C:
		}
		status, _, body, err := f.do(ctx, http.MethodPost, ssoTokenURL, url.Values{
			"grant_type": {"urn:ietf:params:oauth:grant-type:device_code"}, "client_id": {ssoBuildClientID}, "device_code": {deviceCode},
		})
		if err != nil {
			return ssoBuildToken{}, err
		}
		var payload struct {
			AccessToken      string `json:"access_token"`
			RefreshToken     string `json:"refresh_token"`
			IDToken          string `json:"id_token"`
			ExpiresIn        int    `json:"expires_in"`
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return ssoBuildToken{}, fmt.Errorf("解析 xAI OAuth Token: %w", err)
		}
		if status >= 200 && status < 300 && payload.AccessToken != "" {
			if payload.ExpiresIn <= 0 {
				payload.ExpiresIn = 3600
			}
			return ssoBuildToken{AccessToken: payload.AccessToken, RefreshToken: payload.RefreshToken, IDToken: payload.IDToken, ExpiresAt: time.Now().UTC().Add(time.Duration(payload.ExpiresIn) * time.Second)}, nil
		}
		switch payload.Error {
		case "authorization_pending":
			continue
		case "slow_down":
			interval += 5 * time.Second
			continue
		case "access_denied", "expired_token":
			return ssoBuildToken{}, provider.ErrAuthorizationDenied
		default:
			if status >= 400 {
				return ssoBuildToken{}, fmt.Errorf("xAI OAuth Token 失败 (%s): %w", firstValue(payload.ErrorDescription, payload.Error), conversionHTTPError{status: status})
			}
			return ssoBuildToken{}, fmt.Errorf("xAI OAuth Token 失败: %s", firstValue(payload.ErrorDescription, payload.Error, strconv.Itoa(status)))
		}
	}
	return ssoBuildToken{}, fmt.Errorf("xAI Device Flow 轮询超时")
}

func (f *ssoBuildFlow) do(ctx context.Context, method, endpoint string, form url.Values) (int, string, []byte, error) {
	return f.doWithFollow(ctx, method, endpoint, form, true)
}

// doWithFollow 在 follow=false 时遇到 3xx 直接返回状态码与解析后的 Location 作为 finalURL，
// 用于重定向目标域会被 Cloudflare 拦截（accounts.x.ai）的请求。
func (f *ssoBuildFlow) doWithFollow(ctx context.Context, method, endpoint string, form url.Values, follow bool) (int, string, []byte, error) {
	return f.doWithFollowHeaders(ctx, method, endpoint, form, follow, nil)
}

func (f *ssoBuildFlow) doWithFollowHeaders(ctx context.Context, method, endpoint string, form url.Values, follow bool, extraHeaders map[string]string) (int, string, []byte, error) {
	if !safeXAIURL(endpoint) {
		return 0, "", nil, fmt.Errorf("xAI OAuth URL 不安全")
	}
	currentURL := endpoint
	currentMethod := method
	currentForm := form
	for redirects := 0; redirects <= 8; redirects++ {
		var body io.Reader
		if currentForm != nil {
			body = strings.NewReader(currentForm.Encode())
		}
		request, err := http.NewRequestWithContext(ctx, currentMethod, currentURL, body)
		if err != nil {
			return 0, "", nil, err
		}
		request.Header.Set("Accept", "application/json, text/html;q=0.9, */*;q=0.8")
		request.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
		request.Header.Set("User-Agent", f.userAgent)
		request.Header.Set("Cookie", f.cookieHeader())
		if currentMethod == http.MethodGet && ssoDeviceRedirectState(currentURL) == "consent" {
			request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
			request.Header.Set("Referer", "https://auth.x.ai/")
			request.Header.Set("Sec-Fetch-Dest", "document")
			request.Header.Set("Sec-Fetch-Mode", "navigate")
			request.Header.Set("Sec-Fetch-Site", "same-site")
			request.Header.Set("Upgrade-Insecure-Requests", "1")
		}
		for key, value := range extraHeaders {
			request.Header.Set(key, value)
		}
		if currentForm != nil {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		response, err := f.client.Do(request)
		if err != nil {
			return 0, "", nil, err
		}
		f.captureCookies(response)
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxAuthBody+1))
		_ = response.Body.Close()
		if readErr != nil {
			return response.StatusCode, currentURL, nil, readErr
		}
		if len(data) > maxAuthBody {
			return response.StatusCode, currentURL, nil, fmt.Errorf("xAI OAuth 响应超过 2 MiB")
		}
		if response.StatusCode < 300 || response.StatusCode > 399 {
			return response.StatusCode, currentURL, data, nil
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			return response.StatusCode, currentURL, data, fmt.Errorf("xAI OAuth 重定向缺少 Location")
		}
		base, _ := url.Parse(currentURL)
		next, err := url.Parse(location)
		if err != nil {
			return response.StatusCode, currentURL, data, err
		}
		currentURL = base.ResolveReference(next).String()
		if !safeXAIURL(currentURL) {
			return response.StatusCode, currentURL, data, fmt.Errorf("xAI OAuth 重定向到非受信域名")
		}
		if !follow {
			return response.StatusCode, currentURL, data, nil
		}
		if response.StatusCode == http.StatusSeeOther || ((response.StatusCode == http.StatusMovedPermanently || response.StatusCode == http.StatusFound) && currentMethod != http.MethodGet && currentMethod != http.MethodHead) {
			currentMethod = http.MethodGet
			currentForm = nil
		}
	}
	return 0, currentURL, nil, fmt.Errorf("xAI OAuth 重定向次数过多")
}

func (f *ssoBuildFlow) captureCookies(response *http.Response) {
	for _, cookie := range response.Cookies() {
		name := strings.TrimSpace(cookie.Name)
		value := strings.TrimSpace(cookie.Value)
		if name == "" || len(name) > 128 || len(value) > 16384 || strings.ContainsAny(name+value, "\r\n\x00") {
			continue
		}
		if cookie.MaxAge < 0 {
			delete(f.cookies, name)
			continue
		}
		f.cookies[name] = value
	}
}

func ssoDeviceRedirectState(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || !safeXAIURL(raw) {
		return ""
	}
	path := strings.TrimRight(parsed.EscapedPath(), "/")
	switch path {
	case "/oauth2/device/consent":
		return "consent"
	case "/oauth2/device/done":
		return "done"
	case "/sign-in", "/sign-up":
		return "sign-in"
	default:
		return ""
	}
}

func (f *ssoBuildFlow) cookieHeader() string {
	keys := make([]string, 0, len(f.cookies))
	for key := range f.cookies {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+f.cookies[key])
	}
	return strings.Join(parts, "; ")
}

func ssoBuildCookies(token, cloudflareCookies string) map[string]string {
	cookies := make(map[string]string)
	for _, part := range strings.Split(infraegress.BuildSSOCookie(token, cloudflareCookies), ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && name != "" {
			cookies[name] = value
		}
	}
	return cookies
}

func safeXAIURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "x.ai" || strings.HasSuffix(host, ".x.ai")
}

func normalizeSSOToken(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "sso=") {
		value = strings.TrimSpace(value[len("sso="):])
	}
	if token, _, found := strings.Cut(value, ";"); found {
		value = strings.TrimSpace(token)
	}
	return strings.NewReplacer("\r", "", "\n", "", "\x00", "").Replace(value)
}

var (
	consentTokenJSONRe = regexp.MustCompile(`(?i)\\?["']?(?:consentToken|consent_token)\\?["']?\s*[:=]\s*\\?["'](eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\\?["']`)
)

// extractConsentToken 从 consent 页面 HTML 中解析一次性 consent_token：
// 优先取表单隐藏字段 name="consent_token"，回退到 RSC/JSON 里的 consentToken。
func extractConsentToken(body []byte) string {
	tokenizer := html.NewTokenizer(strings.NewReader(string(body)))
	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			break
		}
		if tokenType != html.StartTagToken && tokenType != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		if !strings.EqualFold(token.Data, "input") {
			continue
		}
		var name, value string
		for _, attribute := range token.Attr {
			switch strings.ToLower(attribute.Key) {
			case "name":
				name = attribute.Val
			case "value":
				value = attribute.Val
			}
		}
		if strings.EqualFold(name, "consent_token") && value != "" {
			return value
		}
	}
	if m := consentTokenJSONRe.FindSubmatch(body); m != nil {
		return string(m[1])
	}
	return ""
}

// consentTokenMatchesUserCode binds a consent token to this device request
// when its dc claim is a server-side authorization id (observed 2026-10-20:
// a UUID unrelated to device_code). The consent form echoes our user_code, so
// the token is accepted only from that form, with the consent audience and an
// unexpired exp.
func consentTokenMatchesUserCode(token, pageUserCode, userCode string, now time.Time) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" || userCode == "" || !strings.EqualFold(strings.TrimSpace(pageUserCode), userCode) {
		return false
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var head struct {
		Typ string `json:"typ"`
	}
	if json.Unmarshal(header, &head) != nil || !strings.EqualFold(head.Typ, "consent+jwt") {
		return false
	}
	claims := decodeBuildClaims(token)
	if claimString(claims, "aud") != "consent" || claimString(claims, "dc") == "" {
		return false
	}
	exp, _ := claims["exp"].(float64)
	return exp > 0 && time.Unix(int64(exp), 0).After(now)
}

// extractConsentInput returns the value of a hidden input on the consent form.
func extractConsentInput(body []byte, field string) string {
	tokenizer := html.NewTokenizer(strings.NewReader(string(body)))
	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			return ""
		}
		if tokenType != html.StartTagToken && tokenType != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		if !strings.EqualFold(token.Data, "input") {
			continue
		}
		var name, value string
		for _, attribute := range token.Attr {
			switch strings.ToLower(attribute.Key) {
			case "name":
				name = attribute.Val
			case "value":
				value = attribute.Val
			}
		}
		if strings.EqualFold(name, field) {
			return value
		}
	}
}

func consentTokenMatchesDevice(token, deviceCode string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return false
	}
	return claimString(decodeBuildClaims(token), "dc") == deviceCode && deviceCode != ""
}

// consentTokenValid 校验 consent_token 是否为 xAI 签发的有效 consent 令牌。
// 不能用 device_code 比对：实测（见 docs/chen-fork-SSO-Build-consent差异核对.md §8）token 的
// dc claim 是 UUID，而 device_code 是 opaque 串，两者永不相等，旧判据会恒假误报。
func consentTokenValid(token, expectedSubject string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return false
	}
	claims := decodeBuildClaims(token)
	if claims == nil || claimString(claims, "aud") != consentAudience {
		return false
	}
	if expected := strings.TrimSpace(expectedSubject); expected != "" && claimString(claims, "sub") != expected {
		return false
	}
	if exp := claimNumber(claims, "exp"); exp > 0 && time.Now().After(time.Unix(exp, 0)) {
		return false
	}
	return true
}

func claimNumber(claims map[string]any, key string) int64 {
	if value, ok := claims[key].(float64); ok {
		return int64(value)
	}
	return 0
}

func decodeBuildClaims(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	if json.Unmarshal(data, &claims) != nil {
		return nil
	}
	return claims
}

func claimString(claims map[string]any, key string) string {
	value, _ := claims[key].(string)
	return strings.TrimSpace(value)
}

func firstValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// errBrowserVerificationRequired reports that xAI refused the automated
// device approval because it requires an interactive browser check.
var errBrowserVerificationRequired = errors.New("xAI device approval requires browser verification")

func isBrowserVerificationRequired(body []byte) bool {
	text := strings.ToLower(string(body))
	return strings.Contains(text, "could not verify this browser") || strings.Contains(text, "request could not be verified")
}

type conversionHTTPError struct{ status int }

func (e conversionHTTPError) Error() string { return fmt.Sprintf("xAI OAuth HTTP %d", e.status) }

func conversionStatus(err error) int {
	var statusErr conversionHTTPError
	if errors.As(err, &statusErr) {
		return statusErr.status
	}
	if errors.Is(err, provider.ErrUnauthorized) {
		return http.StatusUnauthorized
	}
	return 0
}

var _ provider.BuildCredentialConverter = (*Adapter)(nil)
var _ ssoBuildHTTPClient = (*infraegress.Lease)(nil)
