"""FlareSolverr-backed managed clearance provider."""

import asyncio
import hashlib
import json
from typing import TypeAlias, cast
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from app.platform.logging.logger import logger
from app.platform.config.snapshot import get_config
from ..models import ClearanceBundle, ClearanceMode

JSONDict: TypeAlias = dict[str, object]
Cookie: TypeAlias = dict[str, object]


def _extract_all_cookies(cookies: list[Cookie]) -> str:
    return "; ".join(f"{cookie.get('name')}={cookie.get('value')}" for cookie in cookies)


class FlareSolverrClearanceProvider:
    """Refresh CF clearance bundles via a FlareSolverr instance."""

    def __init__(self) -> None:
        self._created_sessions: set[str] = set()
        self._session_lock: asyncio.Lock = asyncio.Lock()

    async def refresh_bundle(
        self,
        *,
        affinity_key: str,
        proxy_url:    str,
        target_url:   str = "https://grok.com",
    ) -> ClearanceBundle | None:
        cfg = get_config()
        mode = ClearanceMode.parse(cfg.get_str("proxy.clearance.mode", "none"))
        if mode != ClearanceMode.FLARESOLVERR:
            return None
        fs_url      = cfg.get_str("proxy.clearance.flaresolverr_url", "")
        timeout_sec = cfg.get_int("proxy.clearance.timeout_sec", 60)
        if not fs_url:
            return None

        result = await self._solve(
            fs_url      = fs_url,
            proxy_url   = proxy_url,
            timeout_sec = timeout_sec,
            target_url  = target_url,
        )
        if not result:
            logger.warning(
                "flaresolverr clearance refresh failed: affinity={} proxy={} target={}",
                affinity_key, proxy_url or "<direct>", target_url,
            )
            return None
        host = result.get("clearance_host", "grok.com")

        return ClearanceBundle(
            bundle_id    = f"flaresolverr:{affinity_key}@{host}",
            cf_cookies   = result.get("cookies", ""),
            user_agent   = result.get("user_agent", ""),
            affinity_key = affinity_key,
            clearance_host = host,
        )

    async def _solve(
        self,
        *,
        fs_url:      str,
        proxy_url:   str,
        timeout_sec: int,
        target_url:  str,
    ) -> dict[str, str] | None:
        target = target_url.strip() or "https://grok.com"
        session = self._session_name(target, proxy_url)
        if not await self._ensure_session(
            fs_url      = fs_url,
            session     = session,
            proxy_url   = proxy_url,
            timeout_sec = timeout_sec,
        ):
            return None
        payload: JSONDict = {
            "cmd":        "request.get",
            "url":        target,
            "maxTimeout": timeout_sec * 1000,
            "session":    session,
        }
        if proxy_url:
            payload["proxy"] = {"url": proxy_url}

        body    = json.dumps(payload).encode()
        request = urllib_request.Request(
            f"{fs_url.rstrip('/')}/v1",
            data    = body,
            method  = "POST",
            headers = {"Content-Type": "application/json"},
        )

        try:
            def _post() -> JSONDict:
                with urllib_request.urlopen(request, timeout=timeout_sec + 30) as resp:
                    return cast(JSONDict, json.loads(resp.read().decode()))

            result = await asyncio.to_thread(_post)
            if result.get("status") != "ok":
                logger.warning(
                    "flaresolverr returned non-ok status: status={} message={}",
                    result.get("status"), result.get("message", ""),
                )
                return None

            solution_obj = result.get("solution", {})
            solution = solution_obj if isinstance(solution_obj, dict) else {}
            cookies_obj = solution.get("cookies", [])
            cookies = cast(list[Cookie], cookies_obj if isinstance(cookies_obj, list) else [])
            if not cookies:
                logger.warning("flaresolverr returned no cookies")
                return None

            ua = str(solution.get("userAgent", "") or "")
            host = (urlparse(target).hostname or "").lower()
            filtered = [
                cookie for cookie in cookies
                if not host or not cookie.get("domain") or host.endswith(str(cookie.get("domain", "")).lstrip(".").lower())
            ]
            chosen = filtered or cookies
            return {
                "cookies":    _extract_all_cookies(chosen),
                "user_agent": ua,
                "clearance_host": host or "grok.com",
            }

        except HTTPError as exc:
            body_text = exc.read().decode("utf-8", "replace")[:300]
            logger.warning("flaresolverr http request failed: status={} body={}", exc.code, body_text)
        except URLError as exc:
            logger.warning("flaresolverr connection failed: reason={}", exc.reason)
        except Exception as exc:
            logger.warning("flaresolverr request failed: error={}", exc)

        await self._destroy_session(
            fs_url      = fs_url,
            session     = session,
            timeout_sec = timeout_sec,
        )

        return None

    def _session_name(self, target_url: str, proxy_url: str) -> str:
        host = (urlparse(target_url).hostname or "grok.com").lower()
        source = f"{host}|{proxy_url or '<direct>'}"
        digest = hashlib.sha256(source.encode()).hexdigest()[:16]
        return f"grok2api-{host.replace('.', '-')}-{digest}"

    async def _ensure_session(
        self,
        *,
        fs_url:      str,
        session:     str,
        proxy_url:   str,
        timeout_sec: int,
    ) -> bool:
        async with self._session_lock:
            if session in self._created_sessions:
                return True
            payload: JSONDict = {
                "cmd":     "sessions.create",
                "session": session,
            }
            if proxy_url:
                payload["proxy"] = {"url": proxy_url}
            result = await self._post_json(
                fs_url      = fs_url,
                payload     = payload,
                timeout_sec = min(timeout_sec, 30),
            )
            status = result.get("status") if result else ""
            message = str(result.get("message", "")) if result else ""
            if status == "ok" or "already" in message.lower():
                self._created_sessions.add(session)
                logger.info("flaresolverr session ready: session={}", session)
                return True
            logger.warning(
                "flaresolverr session create failed: session={} status={} message={}",
                session, status, message,
            )
            return False

    async def _destroy_session(
        self,
        *,
        fs_url:      str,
        session:     str,
        timeout_sec: int,
    ) -> None:
        async with self._session_lock:
            self._created_sessions.discard(session)
        payload: JSONDict = {
            "cmd":     "sessions.destroy",
            "session": session,
        }
        result = await self._post_json(
            fs_url      = fs_url,
            payload     = payload,
            timeout_sec = min(timeout_sec, 15),
        )
        status = result.get("status") if result else ""
        if status == "ok":
            logger.info("flaresolverr session destroyed after failed refresh: session={}", session)
            return
        logger.warning("flaresolverr session destroy failed: session={} status={}", session, status)

    async def _post_json(
        self,
        *,
        fs_url:      str,
        payload:     JSONDict,
        timeout_sec: int,
    ) -> JSONDict | None:
        body = json.dumps(payload).encode()
        request = urllib_request.Request(
            f"{fs_url.rstrip('/')}/v1",
            data    = body,
            method  = "POST",
            headers = {"Content-Type": "application/json"},
        )

        def _post() -> JSONDict:
            with urllib_request.urlopen(request, timeout=timeout_sec) as resp:
                return cast(JSONDict, json.loads(resp.read().decode()))

        try:
            return await asyncio.to_thread(_post)
        except HTTPError as exc:
            body_text = exc.read().decode("utf-8", "replace")[:300]
            logger.warning("flaresolverr http request failed: status={} body={}", exc.code, body_text)
        except URLError as exc:
            logger.warning("flaresolverr connection failed: reason={}", exc.reason)
        except Exception as exc:
            logger.warning("flaresolverr request failed: error={}", exc)
        return None


__all__ = ["FlareSolverrClearanceProvider"]
