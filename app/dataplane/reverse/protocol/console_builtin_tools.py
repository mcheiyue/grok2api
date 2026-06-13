"""Canonical set of console.x.ai server-side built-in tool names.

These tools are executed internally by the upstream console.x.ai platform
and may appear as ``function_call`` output items in the response.  They
must **never** be forwarded as client-visible ``tool_calls`` because:

  1. Clients don't define them — they can't handle the call.
  2. Forwarding them causes empty replies or upstream errors.
  3. The upstream already consumes results internally.

The set is intentionally conservative: any new server-side tool xAI adds
should be added here immediately.
"""

# fmt: off
CONSOLE_BUILTIN_TOOLS: frozenset[str] = frozenset({
    "web_search",
    "x_search",
    "x_keyword_search",
    "x_semantic_search",
    "browse_page",
    "search_images",
    "image_search",
    "chatroom_send",
    "code_execution",
    "code_interpreter",
})
# fmt: on


def is_console_builtin_tool(name: str) -> bool:
    """Return True if *name* is a known console built-in tool."""
    return name in CONSOLE_BUILTIN_TOOLS
