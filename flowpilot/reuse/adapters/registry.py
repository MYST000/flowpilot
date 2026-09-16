from __future__ import annotations

from .base import ReuseAdapter
from .tavily import TavilyExtractAdapter, TavilySearchAdapter
from .url_fetch import CurlUrlFetchAdapter

ADAPTERS: dict[str, ReuseAdapter] = {
    "tavily-search": TavilySearchAdapter(),
    "tavily-extract": TavilyExtractAdapter(),
    "terminal": CurlUrlFetchAdapter(),
    "curl": CurlUrlFetchAdapter(),
    "url_fetch": CurlUrlFetchAdapter(),
}


def get_adapter(tool_name: str) -> ReuseAdapter | None:
    return ADAPTERS.get(tool_name)
