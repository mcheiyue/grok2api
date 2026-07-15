package gateway

import (
	"testing"
	"time"
)

func TestConsoleModelCooldownKeySharesMultiAgent(t *testing.T) {
	a := consoleModelCooldownKey("grok-4.20-multi-agent-0309")
	b := consoleModelCooldownKey("grok-4.20-multi-agent-high")
	if a != "console:multi-agent" || b != a {
		t.Fatalf("expected shared multi key, got %q %q", a, b)
	}
	if consoleModelCooldownKey("grok-4.3") != "console:grok-4.3" {
		t.Fatalf("non-multi key wrong")
	}
}

func TestParseConsole429InfoRPMAndRPS(t *testing.T) {
	rpm := parseConsole429Info("Requests per Minute (actual/limit): 80/60")
	if !rpm.IsPerMinuteHit || rpm.PerMinuteActual != 80 || rpm.PerMinuteLimit != 60 {
		t.Fatalf("rpm=%#v", rpm)
	}
	rps := parseConsole429Info("Requests per Second (actual/limit): 5/3")
	if !rps.IsPerSecondHit || rps.IsPerMinuteHit {
		t.Fatalf("rps=%#v", rps)
	}
	unknown := parseConsole429Info("rate limited")
	if unknown.IsPerMinuteHit || unknown.IsPerSecondHit {
		t.Fatalf("unknown=%#v", unknown)
	}
}

func TestConsoleTeamCircuitBreakerTripAndBlock(t *testing.T) {
	fixed := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	consoleCooldownNow = func() time.Time { return fixed }
	t.Cleanup(func() { consoleCooldownNow = time.Now })

	cb := newConsoleTeamCircuitBreaker(75)
	key := consoleModelCooldownKey("grok-4.20-multi-agent-0309")
	if cb.remaining(key) != 0 {
		t.Fatal("expected free")
	}
	d, kind := cb.trip(key, console429Info{PerMinuteActual: 80, PerMinuteLimit: 60, IsPerMinuteHit: true})
	if kind != "rpm" || d != 75*time.Second {
		t.Fatalf("d=%v kind=%s", d, kind)
	}
	// 同家族另一模型名也应 blocked
	other := consoleModelCooldownKey("grok-4.20-multi-agent-low")
	if rem := cb.remaining(other); rem != 75*time.Second {
		t.Fatalf("shared block rem=%v", rem)
	}
	// RPS 短冷却不会缩短已有 RPM
	d2, kind2 := cb.trip(key, console429Info{PerSecondActual: 5, PerSecondLimit: 3, IsPerSecondHit: true})
	if kind2 != "rpm" || d2 < 74*time.Second {
		t.Fatalf("should keep longer rpm, d2=%v kind2=%s", d2, kind2)
	}
}

func TestConsoleTeamCircuitBreakerRPSShort(t *testing.T) {
	fixed := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	consoleCooldownNow = func() time.Time { return fixed }
	t.Cleanup(func() { consoleCooldownNow = time.Now })

	cb := newConsoleTeamCircuitBreaker(75)
	key := consoleModelCooldownKey("grok-4.3")
	d, kind := cb.trip(key, console429Info{PerSecondActual: 3, PerSecondLimit: 3, IsPerSecondHit: true})
	if kind != "rps" || d != 3*time.Second {
		t.Fatalf("d=%v kind=%s", d, kind)
	}
}
