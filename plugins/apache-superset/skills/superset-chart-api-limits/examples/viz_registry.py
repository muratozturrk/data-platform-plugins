"""Which chart types does this Superset frontend register?

`POST /api/v1/chart/` stores any `viz_type` string unvalidated, so the REST API cannot tell whether a chart type
exists. The served JavaScript can: the SPA entry bundle registers every chart plugin as
`new Plugin().configure({key:"<viz_type>"})`. The bundle is public static content
(`/static/assets/manifest.json` lists its files), so a test can read it without a browser.
"""
from __future__ import annotations

import re

import requests

_KEY = re.compile(r'\.configure\(\{key:"([a-z_0-9]+)"\}\)')
_CACHE: dict[str, frozenset[str]] = {}


def registered_viz_keys(base_url: str, timeout: int = 60) -> frozenset[str]:
    """All `viz_type` keys the frontend can render (63 in Superset 4.1.2). Cached per base URL."""
    base_url = base_url.rstrip("/")
    if base_url not in _CACHE:
        manifest = requests.get(f"{base_url}/static/assets/manifest.json", timeout=timeout)
        manifest.raise_for_status()
        keys: set[str] = set()
        for path in manifest.json()["entrypoints"]["spa"]["js"]:
            js = requests.get(f"{base_url}{path}", timeout=timeout)
            js.raise_for_status()
            keys |= set(_KEY.findall(js.text))
        _CACHE[base_url] = frozenset(keys)
    return _CACHE[base_url]
