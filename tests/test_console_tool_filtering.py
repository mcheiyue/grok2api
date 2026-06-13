"""Tests for console built-in tool filtering.

Locks down the filtering behaviour of ``extract_console_tool_calls`` (non-streaming)
and ``ConsoleStreamAdapter`` (streaming) so that server-side tools (web_search,
x_search, code_interpreter, etc.) are never forwarded as ``function_call`` items
to the client, while user-defined tools pass through unchanged.

Scenarios covered per code path:
  1. Allowed user tool forwarded
  2. Built-in tool filtered
  3. Mixed (user + built-in) → only user tool forwarded
  4. No user tools (allowed_names=set()) → zero function_calls forwarded
  5. Backward compat: allowed_names=None → built-in still filtered, user tools pass
"""

from __future__ import annotations

import json

import pytest

from app.dataplane.reverse.protocol.console_builtin_tools import (
    CONSOLE_BUILTIN_TOOLS,
)
from app.dataplane.reverse.protocol.xai_console import (
    ConsoleStreamAdapter,
    extract_console_tool_calls,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _function_call_item(name: str, call_id: str = "", arguments: str = "{}") -> dict:
    return {
        "type": "function_call",
        "call_id": call_id or f"call_{name}",
        "name": name,
        "arguments": arguments,
    }


def _message_item(text: str = "hello") -> dict:
    return {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def _response_json(*items: dict) -> dict:
    return {"output": list(items)}


def _sse_event(event_name: str) -> str:
    return f"event: {event_name}"


def _sse_data(payload: dict) -> str:
    return f"data: {json.dumps(payload)}"


def _fc_start_lines(name: str, item_id: str = "", call_id: str = "") -> list[str]:
    eid = item_id or f"item_{name}"
    cid = call_id or f"call_{name}"
    item = {
        "id": eid,
        "type": "function_call",
        "call_id": cid,
        "name": name,
        "arguments": "",
        "status": "in_progress",
    }
    return [
        _sse_event("response.output_item.added"),
        _sse_data({"type": "response.output_item.added", "output_index": 0, "item": item}),
    ]


def _fc_args_delta(item_id: str, delta: str) -> list[str]:
    return [
        _sse_event("response.function_call_arguments.delta"),
        _sse_data({
            "type": "response.function_call_arguments.delta",
            "item_id": item_id,
            "output_index": 0,
            "delta": delta,
        }),
    ]


def _fc_args_done(item_id: str, arguments: str) -> list[str]:
    return [
        _sse_event("response.function_call_arguments.done"),
        _sse_data({
            "type": "response.function_call_arguments.done",
            "item_id": item_id,
            "output_index": 0,
            "arguments": arguments,
        }),
    ]


def _completed_lines() -> list[str]:
    return [
        _sse_event("response.completed"),
        _sse_data({"type": "response.completed", "response": {}}),
    ]


def _feed_sse(adapter: ConsoleStreamAdapter, lines: list[str]) -> list[dict]:
    results = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("event:"):
            adapter.feed_event(line[6:].strip())
        elif line.startswith("data:"):
            data = line[5:].strip()
            ev = adapter.feed_data(data)
            if ev.get("kind") != "skip":
                results.append(ev)
    return results


# ===================================================================
# Non-streaming: extract_console_tool_calls
# ===================================================================


class TestExtractConsoleToolCalls:

    def test_allowed_user_tool_forwarded(self):
        user_tool_names = {"get_weather"}
        resp = _response_json(
            _function_call_item("get_weather", "call_1", '{"location":"NYC"}'),
            _message_item("Let me check."),
        )
        calls = extract_console_tool_calls(resp, allowed_names=user_tool_names)
        assert len(calls) == 1
        assert calls[0]["function"]["name"] == "get_weather"
        assert calls[0]["function"]["arguments"] == '{"location":"NYC"}'
        assert calls[0]["id"] == "call_1"

    @pytest.mark.parametrize("builtin_name", sorted(CONSOLE_BUILTIN_TOOLS))
    def test_builtin_tool_filtered(self, builtin_name: str):
        user_tool_names = {"get_weather"}
        resp = _response_json(
            _function_call_item(builtin_name, f"call_{builtin_name}"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=user_tool_names)
        assert len(calls) == 0

    def test_mixed_user_and_builtin_only_user_forwarded(self):
        user_tool_names = {"get_weather", "get_time"}
        resp = _response_json(
            _function_call_item("get_weather", "call_1", '{"location":"NYC"}'),
            _function_call_item("web_search", "call_ws"),
            _function_call_item("get_time", "call_2", '{"tz":"UTC"}'),
            _function_call_item("x_search", "call_xs"),
            _function_call_item("code_interpreter", "call_ci"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=user_tool_names)
        names = [c["function"]["name"] for c in calls]
        assert names == ["get_weather", "get_time"]

    def test_empty_allowed_names_suppresses_all(self):
        resp = _response_json(
            _function_call_item("web_search", "call_ws"),
            _function_call_item("get_weather", "call_1"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=set())
        assert calls == []

    def test_allowed_names_none_filters_only_builtin(self):
        resp = _response_json(
            _function_call_item("get_weather", "call_1"),
            _function_call_item("web_search", "call_ws"),
            _function_call_item("custom_tool", "call_2"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=None)
        names = [c["function"]["name"] for c in calls]
        assert "web_search" not in names
        assert "get_weather" in names
        assert "custom_tool" in names

    def test_no_function_call_items_returns_empty(self):
        resp = _response_json(_message_item("plain text"))
        calls = extract_console_tool_calls(resp, allowed_names={"get_weather"})
        assert calls == []

    def test_empty_output_returns_empty(self):
        calls = extract_console_tool_calls({}, allowed_names={"get_weather"})
        assert calls == []

    def test_multiple_user_tools_all_forwarded(self):
        user_tool_names = {"alpha", "beta", "gamma"}
        resp = _response_json(
            _function_call_item("alpha", "c1"),
            _function_call_item("beta", "c2"),
            _function_call_item("gamma", "c3"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=user_tool_names)
        names = {c["function"]["name"] for c in calls}
        assert names == {"alpha", "beta", "gamma"}

    def test_builtin_filtered_even_when_in_allowed_names(self):
        user_tool_names = {"get_weather", "web_search"}
        resp = _response_json(
            _function_call_item("get_weather", "call_1"),
            _function_call_item("web_search", "call_ws"),
        )
        calls = extract_console_tool_calls(resp, allowed_names=user_tool_names)
        names = [c["function"]["name"] for c in calls]
        assert names == ["get_weather"]


# ===================================================================
# Streaming: ConsoleStreamAdapter
# ===================================================================


class TestConsoleStreamAdapterFiltering:

    def test_allowed_user_tool_forwarded_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather"})
        results = _feed_sse(adapter, _fc_start_lines("get_weather", "item_gw", "call_gw"))

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        assert len(starts) == 1
        assert starts[0]["name"] == "get_weather"
        assert len(adapter.tool_calls) == 1
        assert adapter.tool_calls[0]["function"]["name"] == "get_weather"

    @pytest.mark.parametrize("builtin_name", sorted(CONSOLE_BUILTIN_TOOLS))
    def test_builtin_tool_filtered_streaming(self, builtin_name: str):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather"})
        results = _feed_sse(adapter, _fc_start_lines(builtin_name, f"item_{builtin_name}"))

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        assert len(starts) == 0
        assert len(adapter.tool_calls) == 0

    def test_mixed_user_and_builtin_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather", "get_time"})
        lines = []
        lines.extend(_fc_start_lines("get_weather", "item_gw", "call_gw"))
        lines.extend(_fc_start_lines("web_search", "item_ws", "call_ws"))
        lines.extend(_fc_start_lines("get_time", "item_gt", "call_gt"))
        results = _feed_sse(adapter, lines)

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        names = [s["name"] for s in starts]
        assert names == ["get_weather", "get_time"]
        tc_names = [tc["function"]["name"] for tc in adapter.tool_calls]
        assert tc_names == ["get_weather", "get_time"]

    def test_empty_allowed_names_suppresses_all_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names=set())
        results = _feed_sse(adapter, _fc_start_lines("get_weather", "item_gw"))

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        assert len(starts) == 0
        assert len(adapter.tool_calls) == 0

    def test_allowed_names_none_filters_only_builtin_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names=None)
        lines = []
        lines.extend(_fc_start_lines("get_weather", "item_gw", "call_gw"))
        lines.extend(_fc_start_lines("web_search", "item_ws", "call_ws"))
        lines.extend(_fc_start_lines("custom_tool", "item_ct", "call_ct"))
        results = _feed_sse(adapter, lines)

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        names = [s["name"] for s in starts]
        assert "web_search" not in names
        assert "get_weather" in names
        assert "custom_tool" in names

    def test_filtered_tool_args_events_also_skipped(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather"})
        lines = []
        lines.extend(_fc_start_lines("web_search", "item_ws", "call_ws"))
        lines.extend(_fc_args_delta("item_ws", '{"q":"test"}'))
        lines.extend(_fc_args_done("item_ws", '{"q":"test"}'))
        results = _feed_sse(adapter, lines)

        kinds = [r["kind"] for r in results]
        assert "tool_call_start" not in kinds
        assert "tool_call_args" not in kinds
        assert "tool_call_done" not in kinds
        assert len(adapter.tool_calls) == 0

    def test_user_tool_full_lifecycle_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather"})
        lines = []
        lines.extend(_fc_start_lines("get_weather", "item_gw", "call_gw"))
        lines.extend(_fc_args_delta("item_gw", '{"loc'))
        lines.extend(_fc_args_delta("item_gw", 'ation":"NYC"}'))
        lines.extend(_fc_args_done("item_gw", '{"location":"NYC"}'))
        lines.extend(_completed_lines())
        results = _feed_sse(adapter, lines)

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        args_deltas = [r for r in results if r["kind"] == "tool_call_args"]
        dones = [r for r in results if r["kind"] == "tool_call_done"]
        assert len(starts) == 1
        assert len(args_deltas) == 2
        assert len(dones) == 1
        assert adapter.tool_calls[0]["function"]["arguments"] == '{"location":"NYC"}'

    def test_builtin_filtered_even_when_in_allowed_names_streaming(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names={"get_weather", "web_search"})
        lines = []
        lines.extend(_fc_start_lines("get_weather", "item_gw", "call_gw"))
        lines.extend(_fc_start_lines("web_search", "item_ws", "call_ws"))
        results = _feed_sse(adapter, lines)

        starts = [r for r in results if r["kind"] == "tool_call_start"]
        names = [s["name"] for s in starts]
        assert names == ["get_weather"]

    def test_text_events_unaffected_by_tool_filtering(self):
        adapter = ConsoleStreamAdapter(allowed_tool_names=set())
        lines = [
            _sse_event("response.output_text.delta"),
            _sse_data({"type": "response.output_text.delta", "delta": "Hello"}),
        ]
        results = _feed_sse(adapter, lines)

        texts = [r for r in results if r["kind"] == "text"]
        assert len(texts) == 1
        assert texts[0]["content"] == "Hello"
        assert adapter.text_buf == ["Hello"]

    def test_builtin_constant_is_nonempty_frozenset(self):
        assert isinstance(CONSOLE_BUILTIN_TOOLS, frozenset)
        assert len(CONSOLE_BUILTIN_TOOLS) > 0
        assert "web_search" in CONSOLE_BUILTIN_TOOLS
