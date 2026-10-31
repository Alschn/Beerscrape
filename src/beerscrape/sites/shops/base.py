from abc import abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import ClassVar

from beerscrape.core import Handler, Link, Page, PageResult, Site
from beerscrape.models import ShopProduct


@dataclass(frozen=True, slots=True)
class Listing:
    """What a product listing page offers: products and, optionally, the next listing page."""

    product_urls: list[str] = field(default_factory=list)
    next_page_url: str | None = None


class ShopSite(Site[ShopProduct]):
    """An online shop crawled as paginated listings that link to product pages."""

    LISTING: ClassVar[str] = "listing"
    PRODUCT: ClassVar[str] = "product"

    start_urls: ClassVar[Sequence[str]]

    @abstractmethod
    def parse_listing(self, page: Page) -> Listing: ...

    @abstractmethod
    def parse_product(self, page: Page) -> ShopProduct: ...

    def start_links(self) -> Sequence[Link]:
        return [Link(url, self.LISTING) for url in self.start_urls]

    def handlers(self) -> Mapping[str, Handler[ShopProduct]]:
        return {self.LISTING: self._handle_listing, self.PRODUCT: self._handle_product}

    def _handle_listing(self, page: Page) -> PageResult[ShopProduct]:
        listing = self.parse_listing(page)
        links = [Link(url, self.PRODUCT) for url in dict.fromkeys(listing.product_urls)]
        if listing.next_page_url:
            links.append(Link(listing.next_page_url, self.LISTING))
        return PageResult(links=links)

    def _handle_product(self, page: Page) -> PageResult[ShopProduct]:
        return PageResult(items=[self.parse_product(page)])
