from dataclasses import dataclass, field
from functools import cached_property
from urllib.parse import urldefrag, urljoin

from bs4 import BeautifulSoup


@dataclass(frozen=True, slots=True)
class Link:
    """A URL to crawl, tagged with the label of the handler that should process it."""

    url: str
    label: str


@dataclass(frozen=True)
class Page:
    """A fetched HTML document. Sites only ever see pages, never the transport behind them."""

    url: str
    html: str | bytes
    label: str

    @cached_property
    def soup(self) -> BeautifulSoup:
        return BeautifulSoup(self.html, "html.parser")

    @cached_property
    def base_url(self) -> str:
        base = self.soup.find("base", href=True)
        href = base.get("href") if base else None
        return urljoin(self.url, href) if isinstance(href, str) else self.url

    def absolute(self, href: str) -> str:
        """Resolve `href` the way a browser would, honouring `<base href>`, minus any fragment."""
        return urldefrag(urljoin(self.base_url, href.strip())).url


@dataclass(slots=True)
class PageResult[T]:
    """What a handler extracted from a page: finished items and further links to follow."""

    items: list[T] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)
