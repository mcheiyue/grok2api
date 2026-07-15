package gateway

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
)

// 从 Gork console_circuit_breaker 摘入：team 级模型冷却，避免 multi 撞 RPM 后空转换号。
const (
	consoleRPMCooldownSec     = 75
	consoleRPSCooldownSec     = 3
	consoleUnknownCooldownSec = 5
)

// 可注入时钟，单测用。
var consoleCooldownNow = time.Now

type console429Info struct {
	PerSecondActual int
	PerSecondLimit  int
	PerMinuteActual int
	PerMinuteLimit  int
	IsPerSecondHit  bool
	IsPerMinuteHit  bool
}

type consoleCooldownEntry struct {
	until time.Time
	kind  string // rpm | rps | unknown
	info  console429Info
}

// consoleTeamCircuitBreaker 按模型冷却键（multi-agent 家族共享）熔断。
type consoleTeamCircuitBreaker struct {
	mu             sync.Mutex
	byModel        map[string]consoleCooldownEntry
	rpmCooldownSec int
	rpsCooldownSec int
	unknownSec     int
}

func newConsoleTeamCircuitBreaker(rpmCooldownSec int) *consoleTeamCircuitBreaker {
	if rpmCooldownSec <= 0 {
		rpmCooldownSec = consoleRPMCooldownSec
	}
	return &consoleTeamCircuitBreaker{
		byModel:        make(map[string]consoleCooldownEntry),
		rpmCooldownSec: rpmCooldownSec,
		rpsCooldownSec: consoleRPSCooldownSec,
		unknownSec:     consoleUnknownCooldownSec,
	}
}

// consoleModelCooldownKey 将上游模型映射到冷却键；multi-agent 家族共享。
func consoleModelCooldownKey(upstreamModel string) string {
	model := strings.ToLower(strings.TrimSpace(upstreamModel))
	if model == "" {
		return "console:unknown"
	}
	if strings.Contains(model, "multi-agent") {
		return "console:multi-agent"
	}
	return "console:" + model
}

func (cb *consoleTeamCircuitBreaker) remaining(modelKey string) time.Duration {
	if cb == nil {
		return 0
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	entry, ok := cb.byModel[modelKey]
	if !ok {
		return 0
	}
	rem := entry.until.Sub(consoleCooldownNow())
	if rem <= 0 {
		delete(cb.byModel, modelKey)
		return 0
	}
	return rem
}

// trip 记录 429 并设置/延长冷却，返回采用的冷却时长与 kind。
func (cb *consoleTeamCircuitBreaker) trip(modelKey string, info console429Info) (time.Duration, string) {
	if cb == nil {
		return 0, "unknown"
	}
	kind, sec := cooldownKindAndSec(info, cb.rpmCooldownSec, cb.rpsCooldownSec, cb.unknownSec)
	duration := time.Duration(sec) * time.Second
	until := consoleCooldownNow().Add(duration)

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if existing, ok := cb.byModel[modelKey]; ok && existing.until.After(until) {
		return existing.until.Sub(consoleCooldownNow()), existing.kind
	}
	cb.byModel[modelKey] = consoleCooldownEntry{until: until, kind: kind, info: info}
	return duration, kind
}

func cooldownKindAndSec(info console429Info, rpmSec, rpsSec, unknownSec int) (string, int) {
	if info.IsPerMinuteHit {
		return "rpm", rpmSec
	}
	if info.IsPerSecondHit {
		return "rps", rpsSec
	}
	return "unknown", unknownSec
}

var (
	perSecondPattern = regexp.MustCompile(`Requests per Second \(actual/limit\): (\d+)/(\d+)`)
	perMinutePattern = regexp.MustCompile(`Requests per Minute \(actual/limit\): (\d+)/(\d+)`)
)

// parseConsole429Info 解析 console 429 正文，判断 RPS/RPM。
func parseConsole429Info(body string) console429Info {
	var info console429Info
	if matches := perSecondPattern.FindStringSubmatch(body); len(matches) == 3 {
		info.PerSecondActual, _ = strconv.Atoi(matches[1])
		info.PerSecondLimit, _ = strconv.Atoi(matches[2])
		if info.PerSecondLimit > 0 {
			info.IsPerSecondHit = info.PerSecondActual >= info.PerSecondLimit
		}
	}
	if matches := perMinutePattern.FindStringSubmatch(body); len(matches) == 3 {
		info.PerMinuteActual, _ = strconv.Atoi(matches[1])
		info.PerMinuteLimit, _ = strconv.Atoi(matches[2])
		if info.PerMinuteLimit > 0 {
			info.IsPerMinuteHit = info.PerMinuteActual >= info.PerMinuteLimit
		}
	}
	return info
}

func isConsoleProvider(provider accountdomain.Provider) bool {
	return provider == accountdomain.ProviderConsole
}
