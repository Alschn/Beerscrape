from bs4 import BeautifulSoup
from bs4.element import NavigableString

from beerscrape.core import CrawlSettings, Page
from beerscrape.core.parsing import (
    attr_of,
    clean_text,
    paragraphs_of,
    parse_float,
    parse_price,
    parse_volume_ml,
    require,
    split_list,
    text_of,
)
from beerscrape.models import ShopProduct
from beerscrape.sites.shops.base import Listing, ShopSite


class Piwoteka(ShopSite):
    """Craft beer from sklep.piwoteka.pl (an Ebexo shop)."""

    name = "piwoteka"
    settings = CrawlSettings(max_concurrency=3, max_requests_per_minute=60)
    start_urls = ("https://sklep.piwoteka.pl/oferta/piwa?pp=1&rpp=60",)

    def parse_listing(self, page: Page) -> Listing:
        soup = page.soup
        hrefs = (attr_of(a, "href") for a in soup.select(".product-box > a[href]"))
        next_href = attr_of(soup.select_one("ul.pagination a.next"), "href")
        return Listing(
            product_urls=[page.absolute(href) for href in hrefs if href],
            next_page_url=page.absolute(next_href) if next_href else None,
        )

    def parse_product(self, page: Page) -> ShopProduct:
        soup = page.soup
        current = require(parse_price(text_of(soup.select_one("#prCurrent"))), "price")
        old = parse_price(text_of(soup.select_one("#prOld")))
        on_sale = old is not None and old > current
        params = _parameters(soup)
        return ShopProduct(
            url=page.url,
            shop=self.name,
            name=require(text_of(soup.select_one("#pbi h1.name")), "product name"),
            regular_price=old if on_sale and old is not None else current,
            discount_price=current if on_sale else None,
            in_stock=_availability(soup),
            brewery=params.get("browar"),
            styles=split_list(params.get("styl")),
            abv=parse_float(params.get("alkohol")),
            extract=parse_float(params.get("ekstrakt")),
            volume_ml=parse_volume_ml(params.get("pojemność")),
            image_url=attr_of(soup.select_one('meta[property="og:image"]'), "content"),
            description=paragraphs_of(soup.select_one("#description")),
        )


def _parameters(soup: BeautifulSoup) -> dict[str, str]:
    """The icon grid under the product, e.g. <p>Alkohol<span>9,5 %</span></p>, by label."""
    params = {}
    for p in soup.select(".product-details-additional p"):
        label = next((clean_text(s) for s in p.children if isinstance(s, NavigableString)), "")
        if label and (value := text_of(p.span)):
            params[label.casefold()] = value
    return params


def _availability(soup: BeautifulSoup) -> bool | None:
    match attr_of(soup.select_one('meta[property="og:availability"]'), "content"):
        case "in stock":
            return True
        case "out of stock":
            return False
        case _:
            return None
