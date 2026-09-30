from __future__ import annotations

from .base import ReuseAdapter
from .benchmark import BENCHMARK_ADAPTERS
from .browsecomp import BrowseCompSearchAdapter
from .tavily import TavilyExtractAdapter, TavilySearchAdapter, TavilySiteAdapter
from .terminal_url import TerminalUrlFetchAdapter
from .url_fetch import CurlUrlFetchAdapter

ADAPTERS: dict[str, ReuseAdapter] = {
    "search": BENCHMARK_ADAPTERS["benchmark_search_v1"],
    "read_document": BENCHMARK_ADAPTERS["benchmark_read_document_v1"],
    "get_document": BENCHMARK_ADAPTERS["benchmark_get_document_v1"],
    "terminal": TerminalUrlFetchAdapter(),
    "curl": CurlUrlFetchAdapter(),
    "url_fetch": CurlUrlFetchAdapter(),
}

# Retained for explicitly pinned historical profiles, not the current selection.
LEGACY_ADAPTERS: dict[str, ReuseAdapter] = {
    "tavily-search": TavilySearchAdapter(),
    "tavily-extract": TavilyExtractAdapter(),
    "tavily-crawl": TavilySiteAdapter("tavily-crawl"),
    "tavily-map": TavilySiteAdapter("tavily-map"),
}


def get_adapter(tool_name: str, adapter_id: str | None = None) -> ReuseAdapter | None:
    if adapter_id in BENCHMARK_ADAPTERS:
        return BENCHMARK_ADAPTERS[adapter_id]
    if adapter_id == BrowseCompSearchAdapter.adapter_id:
        return BrowseCompSearchAdapter(tool_name)
    legacy = LEGACY_ADAPTERS.get(tool_name)
    if legacy is not None and adapter_id == legacy.adapter_id:
        return legacy
    if adapter_id == "generic_v1":
        return None
    return ADAPTERS.get(tool_name)
