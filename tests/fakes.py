from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path

from beerscrape.core import (
    CrawlResult,
    CrawlSettings,
    Failure,
    Handler,
    Link,
    Page,
    PageResult,
    Site,
)
from beerscrape.models import Item

type Html = str | bytes | Path


class Note(Item):
    title: str


class TinySite(Site[Note]):
    """A minimal site for engine tests: an index page linking to note pages."""

    name = "tiny"
    settings = CrawlSettings(max_concurrency=2, max_requests_per_minute=None, max_retries=0)

    def __init__(self, base_url: str = "https://tiny.test") -> None:
        self.base_url = base_url

    def start_links(self) -> Sequence[Link]:
        return [Link(f"{self.base_url}/", "index")]

    def handlers(self) -> Mapping[str, Handler[Note]]:
        return {"index": self.index, "note": self.note}

    def index(self, page: Page) -> PageResult[Note]:
        hrefs = [a["href"] for a in page.soup.select("a")]
        return PageResult(links=[Link(page.absolute(str(h)), "note") for h in hrefs])

    def note(self, page: Page) -> PageResult[Note]:
        title = page.soup.h1
        if title is None:
            raise ValueError("no title")
        return PageResult(items=[Note(url=page.url, title=title.get_text())])


TINY_INDEX = '<a href="/a"></a><a href="/b"></a><a href="/a"></a><a href="/bad"></a>'


def _read(source: Html) -> str | bytes:
    return source.read_bytes() if isinstance(source, Path) else source


class FixtureEngine:
    """An offline engine serving saved HTML by URL, with the same crawl semantics as Crawlee.

    `fallbacks` maps a label to a page served for any URL of that label without its own
    fixture, so a whole listing can be crawled with one representative detail page.
    """

    def __init__(
        self, pages: Mapping[str, Html], fallbacks: Mapping[str, Html] | None = None
    ) -> None:
        self._pages = dict(pages)
        self._fallbacks = dict(fallbacks or {})
        self.visited: list[Link] = []

    async def run[T: Item](self, site: Site[T]) -> CrawlResult[T]:
        result = CrawlResult[T]()
        queue = deque(site.start_links())
        seen = {link.url for link in queue}

        while queue:
            link = queue.popleft()
            self.visited.append(link)
            source = self._pages.get(link.url, self._fallbacks.get(link.label))
            if source is None:
                result.failures.append(Failure(link.url, link.label, "no fixture for URL"))
                continue
            try:
                page_result = site.handle(Page(url=link.url, html=_read(source), label=link.label))
            except Exception as error:
                result.failures.append(Failure(link.url, link.label, repr(error)))
                continue
            result.items.extend(page_result.items)
            for new_link in page_result.links:
                if new_link.url not in seen:
                    seen.add(new_link.url)
                    queue.append(new_link)

        return result
