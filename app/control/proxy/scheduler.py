"""Proxy clearance refresh scheduler.

Periodically refreshes ClearanceBundles for managed (FlareSolverr) mode.
Previously inline in ProxyDirectory; extracted for separation of concerns.

Stage 3 hardening: scheduler is now cooldown-aware — it skips keys that are
still in cooldown, respects half-open probe state, and adapts the refresh
interval based on per-key health.
"""

import asyncio

from app.platform.logging.logger import logger
from app.platform.config.snapshot import get_config
from app.platform.runtime.clock import now_ms
from app.control.proxy import ProxyDirectory, BundleKey


class ProxyClearanceScheduler:
    """Periodically refreshes proxy clearance bundles."""

    def __init__(self, directory: ProxyDirectory) -> None:
        self._directory = directory
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("proxy clearance scheduler started")

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("proxy clearance scheduler stopped")

    async def _loop(self) -> None:
        if self._startup_warm_up_enabled():
            await self._warm_up()
        else:
            logger.info("proxy clearance warm-up skipped by config")
        while self._running:
            try:
                interval = self._get_adaptive_interval()
                await asyncio.sleep(interval)
                if not self._running:
                    break
                await self._refresh()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "proxy clearance scheduler loop failed: error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(60)

    async def _warm_up(self) -> None:
        try:
            await self._directory.load()
            await self._directory.warm_up()
            logger.debug("proxy clearance warm-up completed")
        except Exception as exc:
            logger.warning("proxy clearance warm-up failed: error={}", exc)

    async def _refresh(self) -> None:
        try:
            await self._directory.load()
            skip_keys = self._get_cooldown_keys()
            await self._directory.refresh_clearance_filtered(skip_keys=skip_keys)
            logger.debug(
                "proxy clearance refresh completed: skipped_cooldown={}",
                len(skip_keys),
            )
        except Exception as exc:
            logger.warning("proxy clearance refresh failed: error={}", exc)

    def _get_cooldown_keys(self) -> set[BundleKey]:
        now = now_ms()
        cooldown = self._directory.refresh_backoff_until
        return {key for key, until in cooldown.items() if until > now}

    def _get_half_open_keys(self) -> set[BundleKey]:
        _, max_fails, _, _, _ = self._backoff_config()
        failures = self._directory.failure_counts
        return {key for key, count in failures.items() if count >= max_fails}

    def _get_adaptive_interval(self) -> int:
        base = self._get_interval()
        now = now_ms()
        cooldown = self._directory.refresh_backoff_until
        failures = self._directory.failure_counts
        bundles = self._directory.bundles

        cooldown_count = sum(1 for until in cooldown.values() if until > now)
        half_open = self._get_half_open_keys()
        total = len(bundles) or 1

        if half_open:
            return max(30, base // 4)
        if cooldown_count > total // 2:
            return max(60, base // 2)
        return base

    def _get_interval(self) -> int:
        cfg = get_config()
        return cfg.get_int("proxy.clearance.refresh_interval", 600)

    def _startup_warm_up_enabled(self) -> bool:
        cfg = get_config()
        return cfg.get_bool("proxy.clearance.startup_warmup", True)

    def _backoff_config(self) -> tuple[int, int, float, int, int]:
        cfg = get_config()
        base_sec = max(1, cfg.get_int("proxy.clearance.failure_cooldown_sec", 300))
        max_fails = max(1, cfg.get_int("proxy.clearance.max_consecutive_failures", 5))
        multiplier = max(1.0, cfg.get_float("proxy.clearance.backoff_multiplier", 2.0))
        max_sec = max(base_sec, cfg.get_int("proxy.clearance.max_cooldown_sec", 3600))
        half_open_sec = max(1, cfg.get_int("proxy.clearance.half_open_probe_sec", 30))
        return base_sec, max_fails, multiplier, max_sec, half_open_sec


__all__ = ["ProxyClearanceScheduler"]
