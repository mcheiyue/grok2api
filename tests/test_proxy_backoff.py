"""Tests for ProxyDirectory exponential backoff and half-open probe logic.

Covers:
  1. Consecutive failure counting increments correctly
  2. Success resets failure count and backoff
  3. Exponential backoff doubles per consecutive failure
  4. Backoff capped at max_cooldown_sec
  5. Half-open probe: max_consecutive_failures triggers short probe cooldown
  6. During cooldown, stale bundle is returned
  7. After cooldown expires, no bundle returned (allows fresh refresh)
  8. Independent keys have independent failure state
"""

from __future__ import annotations

import math
from unittest.mock import patch, MagicMock

from app.control.proxy import ProxyDirectory, BundleKey
from app.control.proxy.models import (
    ClearanceBundle,
    ClearanceBundleState,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bundle(
    bundle_id: str = "test-bundle",
    cf_cookies: str = "cf_test",
    user_agent: str = "Mozilla/5.0",
    state: ClearanceBundleState = ClearanceBundleState.VALID,
) -> ClearanceBundle:
    return ClearanceBundle(
        bundle_id=bundle_id,
        cf_cookies=cf_cookies,
        user_agent=user_agent,
        state=state,
        affinity_key="direct",
        clearance_host="grok.com",
    )


def _make_config_mock(
    failure_cooldown_sec: int = 60,
    max_cooldown_sec: int = 600,
    max_consecutive_failures: int = 5,
    backoff_multiplier: float = 2.0,
    half_open_probe_sec: int = 30,
) -> MagicMock:
    cfg = MagicMock()
    _int_map = {
        "proxy.clearance.failure_cooldown_sec": failure_cooldown_sec,
        "proxy.clearance.max_cooldown_sec": max_cooldown_sec,
        "proxy.clearance.max_consecutive_failures": max_consecutive_failures,
        "proxy.clearance.half_open_probe_sec": half_open_probe_sec,
        "proxy.clearance.timeout_sec": 60,
    }
    _float_map = {
        "proxy.clearance.backoff_multiplier": backoff_multiplier,
    }
    cfg.get_int = MagicMock(
        side_effect=lambda key, default=0: _int_map.get(key, default)
    )
    cfg.get_float = MagicMock(
        side_effect=lambda key, default=0.0: _float_map.get(key, default)
    )
    cfg.get_str = MagicMock(return_value="")
    cfg.get_list = MagicMock(return_value=[])
    return cfg


def _expected_cooldown_ms(
    n: int,
    base_sec: int = 60,
    max_sec: int = 600,
    max_fails: int = 5,
    multiplier: float = 2.0,
    half_open_sec: int = 30,
) -> int:
    effective_base = max(1, base_sec)
    if n >= max_fails:
        return max(1, half_open_sec) * 1000
    return int(min(max_sec, effective_base * math.pow(multiplier, n - 1)) * 1000)


# ---------------------------------------------------------------------------
# Test: failure count increments on each record
# ---------------------------------------------------------------------------


def test_failure_count_increments_on_record():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    mock_cfg = _make_config_mock()
    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=1000_000):
        for expected in range(1, 6):
            directory._record_refresh_failure_locked(key)
            assert directory._failure_counts[key] == expected


# ---------------------------------------------------------------------------
# Test: success resets failure count and backoff
# ---------------------------------------------------------------------------


def test_success_resets_failure_count_and_backoff():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    mock_cfg = _make_config_mock()
    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=1000_000):
        directory._record_refresh_failure_locked(key)
        directory._record_refresh_failure_locked(key)
        directory._record_refresh_failure_locked(key)
    assert directory._failure_counts[key] == 3
    assert key in directory._refresh_backoff_until

    directory._record_refresh_success_locked(key)

    assert key not in directory._failure_counts
    assert key not in directory._refresh_backoff_until


# ---------------------------------------------------------------------------
# Test: cooldown bypass — no cookies bundle should not trigger refresh
# ---------------------------------------------------------------------------


def test_cooldown_bypass_no_cookies_skips_refresh():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock(failure_cooldown_sec=1800, max_cooldown_sec=7200,
                                  max_consecutive_failures=3, half_open_probe_sec=1800)

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms):
        directory._record_refresh_failure_locked(key)

    cooldown_until = directory._refresh_backoff_until[key]
    assert cooldown_until > base_ms

    with patch("app.control.proxy.now_ms", return_value=cooldown_until - 1000):
        result = directory._get_cooldown_bundle_locked(key, None)

    assert result is None


# ---------------------------------------------------------------------------
# Test: exponential backoff doubles per consecutive failure
# ---------------------------------------------------------------------------


def test_exponential_backoff_doubles():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock(failure_cooldown_sec=60, max_cooldown_sec=600,
                                  max_consecutive_failures=10)

    for n in range(1, 6):
        with patch("app.control.proxy.get_config", return_value=mock_cfg), \
             patch("app.control.proxy.now_ms", return_value=base_ms + n):
            directory._record_refresh_failure_locked(key)

        expected_ms = _expected_cooldown_ms(n, base_sec=60, max_sec=600,
                                             max_fails=10, multiplier=2.0)
        actual_until = directory._refresh_backoff_until[key]
        actual_ms = actual_until - (base_ms + n)
        assert actual_ms == expected_ms, f"failure #{n}: expected {expected_ms}ms, got {actual_ms}ms"


# ---------------------------------------------------------------------------
# Test: backoff capped at max_cooldown_sec
# ---------------------------------------------------------------------------


def test_backoff_capped_at_max():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock(failure_cooldown_sec=60, max_cooldown_sec=300,
                                  max_consecutive_failures=20)

    for n in range(1, 8):
        with patch("app.control.proxy.get_config", return_value=mock_cfg), \
             patch("app.control.proxy.now_ms", return_value=base_ms + n):
            directory._record_refresh_failure_locked(key)

    expected_ms = _expected_cooldown_ms(7, base_sec=60, max_sec=300,
                                         max_fails=20, multiplier=2.0)
    assert expected_ms == 300_000
    actual_until = directory._refresh_backoff_until[key]
    actual_ms = actual_until - (base_ms + 7)
    assert actual_ms == 300_000


# ---------------------------------------------------------------------------
# Test: half-open probe — max_consecutive_failures triggers short cooldown
# ---------------------------------------------------------------------------


def test_half_open_probe_triggers_short_cooldown_at_max_failures():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock(failure_cooldown_sec=60, max_cooldown_sec=600,
                                  max_consecutive_failures=5, half_open_probe_sec=30)

    for n in range(1, 5):
        with patch("app.control.proxy.get_config", return_value=mock_cfg), \
             patch("app.control.proxy.now_ms", return_value=base_ms + n):
            directory._record_refresh_failure_locked(key)
        assert directory._failure_counts[key] == n

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms + 5):
        directory._record_refresh_failure_locked(key)

    assert directory._failure_counts[key] == 5
    expected_ms = _expected_cooldown_ms(5, base_sec=60, max_sec=600,
                                         max_fails=5, half_open_sec=30)
    assert expected_ms == 30_000
    actual_until = directory._refresh_backoff_until[key]
    actual_ms = actual_until - (base_ms + 5)
    assert actual_ms == 30_000


# ---------------------------------------------------------------------------
# Test: during cooldown, stale bundle is returned
# ---------------------------------------------------------------------------


def test_during_cooldown_returns_stale_bundle():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")
    bundle = _make_bundle(
        state=ClearanceBundleState.INVALID,
        cf_cookies="cf_stale",
        user_agent="Mozilla/5.0",
    )

    base_ms = 1000_000
    mock_cfg = _make_config_mock()

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms):
        directory._record_refresh_failure_locked(key)

    directory._bundles[key] = bundle
    cooldown_until = directory._refresh_backoff_until[key]

    with patch("app.control.proxy.now_ms", return_value=cooldown_until - 1000):
        result = directory._get_cooldown_bundle_locked(key, bundle)

    assert result is not None
    assert result.state == ClearanceBundleState.STALE
    assert result.cf_cookies == "cf_stale"


# ---------------------------------------------------------------------------
# Test: after cooldown expires, no bundle returned (fresh refresh allowed)
# ---------------------------------------------------------------------------


def test_after_cooldown_expires_returns_none():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")
    bundle = _make_bundle(state=ClearanceBundleState.INVALID)

    base_ms = 1000_000
    mock_cfg = _make_config_mock()

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms):
        directory._record_refresh_failure_locked(key)

    directory._bundles[key] = bundle
    cooldown_until = directory._refresh_backoff_until[key]

    with patch("app.control.proxy.now_ms", return_value=cooldown_until + 1):
        result = directory._get_cooldown_bundle_locked(key, bundle)

    assert result is None
    assert key not in directory._refresh_backoff_until


# ---------------------------------------------------------------------------
# Test: independent keys have independent failure state
# ---------------------------------------------------------------------------


def test_independent_keys_independent_state():
    directory = ProxyDirectory()
    key_a: BundleKey = ("proxy-a", "grok.com")
    key_b: BundleKey = ("proxy-b", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock(failure_cooldown_sec=60, max_cooldown_sec=600,
                                  max_consecutive_failures=10)

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms):
        directory._record_refresh_failure_locked(key_a)
        directory._record_refresh_failure_locked(key_a)
        directory._record_refresh_failure_locked(key_a)
        directory._record_refresh_failure_locked(key_b)

    assert directory._failure_counts[key_a] == 3
    assert directory._failure_counts[key_b] == 1

    expected_a = _expected_cooldown_ms(3, base_sec=60, max_sec=600, max_fails=10)
    expected_b = _expected_cooldown_ms(1, base_sec=60, max_sec=600, max_fails=10)

    actual_a = directory._refresh_backoff_until[key_a] - base_ms
    actual_b = directory._refresh_backoff_until[key_b] - base_ms
    assert actual_a == expected_a
    assert actual_b == expected_b


# ---------------------------------------------------------------------------
# Test: success on one key does not affect another
# ---------------------------------------------------------------------------


def test_success_on_one_key_preserves_other():
    directory = ProxyDirectory()
    key_a: BundleKey = ("proxy-a", "grok.com")
    key_b: BundleKey = ("proxy-b", "grok.com")

    base_ms = 1000_000
    mock_cfg = _make_config_mock()

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=base_ms):
        directory._record_refresh_failure_locked(key_a)
        directory._record_refresh_failure_locked(key_a)
        directory._record_refresh_failure_locked(key_b)

    directory._record_refresh_success_locked(key_a)

    assert key_a not in directory._failure_counts
    assert key_a not in directory._refresh_backoff_until
    assert directory._failure_counts[key_b] == 1
    assert key_b in directory._refresh_backoff_until


# ---------------------------------------------------------------------------
# Test: no backoff when cooldown is zero
# ---------------------------------------------------------------------------


def test_record_failure_minimum_backoff_when_cooldown_zero():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    mock_cfg = _make_config_mock(failure_cooldown_sec=0, max_cooldown_sec=600)
    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=1000_000):
        directory._record_refresh_failure_locked(key)

    assert directory._failure_counts[key] == 1
    assert directory._refresh_backoff_until[key] == 1000_000 + 1000


# ---------------------------------------------------------------------------
# Test: failure count persists across multiple record calls
# ---------------------------------------------------------------------------


def test_failure_count_persists_across_calls():
    directory = ProxyDirectory()
    key: BundleKey = ("direct", "grok.com")

    mock_cfg = _make_config_mock()
    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=1000_000):
        directory._record_refresh_failure_locked(key)
        directory._record_refresh_failure_locked(key)

    assert directory._failure_counts[key] == 2

    with patch("app.control.proxy.get_config", return_value=mock_cfg), \
         patch("app.control.proxy.now_ms", return_value=2000_000):
        directory._record_refresh_failure_locked(key)

    assert directory._failure_counts[key] == 3
