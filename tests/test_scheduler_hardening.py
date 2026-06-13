"""Tests for ProxyClearanceScheduler Stage 3 hardening.

Covers:
  1. Scheduler skips keys in cooldown during refresh
  2. Adaptive interval shortens when keys are in half-open state
  3. Adaptive interval shortens when > 50% of keys are in cooldown
  4. Adaptive interval returns base when all keys are healthy
  5. Cooldown keys are correctly identified from ProxyDirectory state
  6. Half-open keys are correctly identified from failure counts
"""

from __future__ import annotations

import asyncio
import math
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from app.control.proxy import ProxyDirectory, BundleKey
from app.control.proxy.models import (
    ClearanceBundle,
    ClearanceBundleState,
    ClearanceMode,
)
from app.control.proxy.scheduler import ProxyClearanceScheduler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bundle(
    bundle_id: str = "test-bundle",
    cf_cookies: str = "cf_test",
    user_agent: str = "Mozilla/5.0",
    state: ClearanceBundleState = ClearanceBundleState.VALID,
    affinity_key: str = "direct",
    clearance_host: str = "grok.com",
) -> ClearanceBundle:
    return ClearanceBundle(
        bundle_id=bundle_id,
        cf_cookies=cf_cookies,
        user_agent=user_agent,
        state=state,
        affinity_key=affinity_key,
        clearance_host=clearance_host,
    )


def _make_config_mock(
    failure_cooldown_sec: int = 60,
    max_cooldown_sec: int = 600,
    max_consecutive_failures: int = 5,
    backoff_multiplier: float = 2.0,
    half_open_probe_sec: int = 30,
    refresh_interval: int = 600,
    startup_warmup: bool = False,
) -> MagicMock:
    cfg = MagicMock()
    _int_map = {
        "proxy.clearance.failure_cooldown_sec": failure_cooldown_sec,
        "proxy.clearance.max_cooldown_sec": max_cooldown_sec,
        "proxy.clearance.max_consecutive_failures": max_consecutive_failures,
        "proxy.clearance.half_open_probe_sec": half_open_probe_sec,
        "proxy.clearance.refresh_interval": refresh_interval,
        "proxy.clearance.timeout_sec": 60,
    }
    _float_map = {
        "proxy.clearance.backoff_multiplier": backoff_multiplier,
    }
    _bool_map = {
        "proxy.clearance.startup_warmup": startup_warmup,
    }
    cfg.get_int = MagicMock(
        side_effect=lambda key, default=0: _int_map.get(key, default)
    )
    cfg.get_float = MagicMock(
        side_effect=lambda key, default=0.0: _float_map.get(key, default)
    )
    cfg.get_bool = MagicMock(
        side_effect=lambda key, default=False: _bool_map.get(key, default)
    )
    cfg.get_str = MagicMock(return_value="")
    cfg.get_list = MagicMock(return_value=[])
    return cfg


def _setup_directory_with_cooldown(
    directory: ProxyDirectory,
    keys: list[BundleKey],
    now_ms_value: int = 1_000_000,
    cooldown_until_ms: int = 2_000_000,
    failure_count: int = 3,
) -> None:
    mock_cfg = _make_config_mock()
    for key in keys:
        directory._bundles[key] = _make_bundle(
            bundle_id=f"bundle:{key[0]}@{key[1]}",
            affinity_key=key[0],
            clearance_host=key[1],
        )
        with patch("app.control.proxy.get_config", return_value=mock_cfg), \
             patch("app.control.proxy.now_ms", return_value=now_ms_value):
            for _ in range(failure_count):
                directory._record_refresh_failure_locked(key)
        directory._refresh_backoff_until[key] = cooldown_until_ms


_SCHEDULER_NOW_PATCH = "app.control.proxy.scheduler.now_ms"


# ---------------------------------------------------------------------------
# Test: scheduler skips keys in cooldown
# ---------------------------------------------------------------------------


def test_scheduler_skips_cooldown_keys():
    directory = ProxyDirectory()
    key_a: BundleKey = ("proxy-a", "grok.com")
    key_b: BundleKey = ("proxy-b", "grok.com")

    mock_cfg = _make_config_mock(startup_warmup=False)
    now = 1_000_000

    _setup_directory_with_cooldown(
        directory, [key_a], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=2,
    )
    directory._bundles[key_b] = _make_bundle(
        bundle_id="bundle-b", affinity_key=key_b[0], clearance_host=key_b[1],
    )

    scheduler = ProxyClearanceScheduler(directory)

    with patch(_SCHEDULER_NOW_PATCH, return_value=now):
        skip = scheduler._get_cooldown_keys()

    assert key_a in skip
    assert key_b not in skip


# ---------------------------------------------------------------------------
# Test: adaptive interval shortens for half-open keys
# ---------------------------------------------------------------------------


def test_adaptive_interval_shortens_for_half_open():
    directory = ProxyDirectory()
    key: BundleKey = ("proxy-a", "grok.com")

    mock_cfg = _make_config_mock(
        refresh_interval=600, max_consecutive_failures=5,
        failure_cooldown_sec=60, half_open_probe_sec=30,
    )
    now = 1_000_000

    _setup_directory_with_cooldown(
        directory, [key], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=5,
    )

    scheduler = ProxyClearanceScheduler(directory)

    with patch(_SCHEDULER_NOW_PATCH, return_value=now), \
         patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg):
        interval = scheduler._get_adaptive_interval()

    assert interval == max(30, 600 // 4)


# ---------------------------------------------------------------------------
# Test: adaptive interval shortens when > 50% keys in cooldown
# ---------------------------------------------------------------------------


def test_adaptive_interval_shortens_when_majority_cooldown():
    directory = ProxyDirectory()
    keys = [("proxy-a", "grok.com"), ("proxy-b", "grok.com")]
    now = 1_000_000

    mock_cfg = _make_config_mock(refresh_interval=600, max_consecutive_failures=10)

    for k in keys:
        _setup_directory_with_cooldown(
            directory, [k], now_ms_value=now,
            cooldown_until_ms=now + 60_000, failure_count=1,
        )

    scheduler = ProxyClearanceScheduler(directory)

    with patch(_SCHEDULER_NOW_PATCH, return_value=now), \
         patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg):
        interval = scheduler._get_adaptive_interval()

    assert interval == max(60, 600 // 2)


# ---------------------------------------------------------------------------
# Test: adaptive interval returns base when healthy
# ---------------------------------------------------------------------------


def test_adaptive_interval_returns_base_when_healthy():
    directory = ProxyDirectory()
    mock_cfg = _make_config_mock(refresh_interval=600)

    scheduler = ProxyClearanceScheduler(directory)

    with patch(_SCHEDULER_NOW_PATCH, return_value=1_000_000), \
         patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg):
        interval = scheduler._get_adaptive_interval()

    assert interval == 600


# ---------------------------------------------------------------------------
# Test: cooldown keys correctly identified
# ---------------------------------------------------------------------------


def test_cooldown_keys_identified_correctly():
    directory = ProxyDirectory()
    key_active: BundleKey = ("proxy-a", "grok.com")
    key_expired: BundleKey = ("proxy-b", "grok.com")
    now = 1_000_000

    _setup_directory_with_cooldown(
        directory, [key_active], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=2,
    )
    directory._refresh_backoff_until[key_expired] = now - 1000

    scheduler = ProxyClearanceScheduler(directory)

    with patch(_SCHEDULER_NOW_PATCH, return_value=now):
        skip = scheduler._get_cooldown_keys()

    assert key_active in skip
    assert key_expired not in skip


# ---------------------------------------------------------------------------
# Test: half-open keys identified correctly
# ---------------------------------------------------------------------------


def test_half_open_keys_identified_correctly():
    directory = ProxyDirectory()
    key_ho: BundleKey = ("proxy-a", "grok.com")
    key_normal: BundleKey = ("proxy-b", "grok.com")

    mock_cfg = _make_config_mock(max_consecutive_failures=5)
    now = 1_000_000

    _setup_directory_with_cooldown(
        directory, [key_ho], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=5,
    )
    _setup_directory_with_cooldown(
        directory, [key_normal], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=2,
    )

    scheduler = ProxyClearanceScheduler(directory)

    with patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg):
        half_open = scheduler._get_half_open_keys()

    assert key_ho in half_open
    assert key_normal not in half_open


# ---------------------------------------------------------------------------
# Test: refresh calls filtered method
# ---------------------------------------------------------------------------


def test_refresh_calls_filtered_method():
    directory = ProxyDirectory()
    key_a: BundleKey = ("proxy-a", "grok.com")
    key_b: BundleKey = ("proxy-b", "grok.com")
    now = 1_000_000

    _setup_directory_with_cooldown(
        directory, [key_a], now_ms_value=now,
        cooldown_until_ms=now + 60_000, failure_count=2,
    )
    directory._bundles[key_b] = _make_bundle(
        bundle_id="bundle-b", affinity_key=key_b[0], clearance_host=key_b[1],
    )

    mock_cfg = _make_config_mock(startup_warmup=False)
    directory.refresh_clearance_filtered = AsyncMock()

    scheduler = ProxyClearanceScheduler(directory)

    async def _run():
        with patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg), \
             patch(_SCHEDULER_NOW_PATCH, return_value=now), \
             patch.object(directory, "load", new_callable=AsyncMock):
            await scheduler._refresh()

    asyncio.run(_run())

    directory.refresh_clearance_filtered.assert_called_once()
    call_kwargs = directory.refresh_clearance_filtered.call_args
    assert key_a in call_kwargs.kwargs["skip_keys"]
    assert key_b not in call_kwargs.kwargs["skip_keys"]


# ---------------------------------------------------------------------------
# Test: refresh skips no keys when none in cooldown
# ---------------------------------------------------------------------------


def test_refresh_skips_no_keys_when_healthy():
    directory = ProxyDirectory()
    mock_cfg = _make_config_mock(startup_warmup=False)
    directory.refresh_clearance_filtered = AsyncMock()

    scheduler = ProxyClearanceScheduler(directory)

    async def _run():
        with patch("app.control.proxy.scheduler.get_config", return_value=mock_cfg), \
             patch(_SCHEDULER_NOW_PATCH, return_value=1_000_000), \
             patch.object(directory, "load", new_callable=AsyncMock):
            await scheduler._refresh()

    asyncio.run(_run())

    directory.refresh_clearance_filtered.assert_called_once()
    call_kwargs = directory.refresh_clearance_filtered.call_args
    assert call_kwargs.kwargs["skip_keys"] == set()
