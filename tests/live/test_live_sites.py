"""Smoke tests against the real websites, to notice when a site changes its markup.

Excluded by default; run with `pytest -m live` (CI does so on a schedule).
"""

import pytest

from beerscrape.core.crawlee_engine import CrawleeEngine
from beerscrape.registry import SITES, get_site

pytestmark = pytest.mark.live


@pytest.mark.parametrize("site_name", sorted(SITES))
async def test_site_still_parses(site_name: str):
    result = await CrawleeEngine(max_requests=6).run(get_site(site_name))

    assert result.failures == []
    assert result.items, "no items scraped: listing markup probably changed"
