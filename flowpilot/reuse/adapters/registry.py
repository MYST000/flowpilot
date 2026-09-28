from __future__ import annotations

from .base import ReuseAdapter
from .browsecomp import BrowseCompSearchAdapter
from .tavily import TavilyExtractAdapter, TavilySearchAdapter, TavilySiteAdapter
from .terminal_url import TerminalUrlFetchAdapter
from .url_fetch import CurlUrlFetchAdapter

ADAPTERS: dict[str, ReuseAdapter] = {
    "tavily-search": TavilySearchAdapter(),
    "tavily-extract": TavilyExtractAdapter(),
    "tavily-crawl": TavilySiteAdapter("tavily-crawl"),
    "tavily-map": TavilySiteAdapter("tavily-map"),
    "terminal": TerminalUrlFetchAdapter(),
    "curl": CurlUrlFetchAdapter(),
    "url_fetch": CurlUrlFetchAdapter(),
}


def get_adapter(tool_name: str, adapter_id: str | None = None) -> ReuseAdapter | None:
    if adapter_id == BrowseCompSearchAdapter.adapter_id:
        return BrowseCompSearchAdapter(tool_name)
    return ADAPTERS.get(tool_name)
