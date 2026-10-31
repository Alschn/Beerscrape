"""Building blocks shared by shops running on WordPress + WooCommerce."""

import json
from decimal import Decimal
from typing import Any, ClassVar

from bs4 import BeautifulSoup
from bs4.element import Tag

from beerscrape.core import Page
from beerscrape.core.parsing import attr_of, parse_price, text_of
from beerscrape.sites.shops.base import Listing, ShopSite


class WooCommerceShop(ShopSite):
    """A shop with WooCommerce's default listing markup.

    Subclasses provide `start_urls` and `parse_product`, using the helpers below for the
    parts of a product page that WooCommerce renders the same way everywhere.
    """

    product_link_selector: ClassVar[str] = "a.woocommerce-LoopProduct-link"
    next_page_selector: ClassVar[str] = "a.next.page-numbers"

    def parse_listing(self, page: Page) -> Listing:
        hrefs = (attr_of(a, "href") for a in page.soup.select(self.product_link_selector))
        next_href = attr_of(page.soup.select_one(self.next_page_selector), "href")
        return Listing(
            product_urls=[page.absolute(href) for href in hrefs if href],
            next_page_url=page.absolute(next_href) if next_href else None,
        )

    @staticmethod
    def prices(price: Tag | None) -> tuple[Decimal | None, Decimal | None]:
        """(regular, discount) from a `.price` element; discount is None when not on sale."""
        if price is None:
            return None, None
        amount = ".woocommerce-Price-amount"
        if (regular := price.select_one(f"del {amount}")) and (
            sale := price.select_one(f"ins {amount}")
        ):
            return parse_price(text_of(regular)), parse_price(text_of(sale))
        return parse_price(text_of(price.select_one(amount))), None

    @staticmethod
    def attributes(soup: BeautifulSoup) -> dict[str, str]:
        """The product attributes table, keyed by lowercase label (e.g. 'alkohol')."""
        table = soup.select_one("table.woocommerce-product-attributes")
        rows = table.select("tr") if table else []
        attributes = {}
        for row in rows:
            label, value = text_of(row.th), text_of(row.td, separator="")
            if label and value:
                attributes[label.casefold()] = value
        return attributes

    @staticmethod
    def json_ld_product(soup: BeautifulSoup) -> dict[str, Any]:
        """The schema.org Product node from the page's JSON-LD, or {} when there is none."""
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.get_text())
            except ValueError:
                continue
            nodes = data.get("@graph", [data]) if isinstance(data, dict) else data
            for node in nodes if isinstance(nodes, list) else []:
                types = node.get("@type") if isinstance(node, dict) else None
                if types == "Product" or (isinstance(types, list) and "Product" in types):
                    return node
        return {}

    @staticmethod
    def availability(offer: dict[str, Any]) -> bool | None:
        value = str(offer.get("availability", ""))
        if value.endswith("/InStock"):
            return True
        if value.endswith(("/OutOfStock", "/SoldOut", "/Discontinued")):
            return False
        return None
