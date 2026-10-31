from typing import Any

from beerscrape.core import CrawlSettings, Page
from beerscrape.core.parsing import (
    paragraphs_of,
    parse_float,
    parse_volume_ml,
    require,
    split_list,
    text_of,
)
from beerscrape.models import ShopProduct
from beerscrape.sites.shops.woocommerce import WooCommerceShop


class SklepImpuls(WooCommerceShop):
    """Craft beer (and some glassware and gift sets) from sklepimpuls.pl."""

    name = "sklep-impuls"
    settings = CrawlSettings(max_concurrency=4, max_requests_per_minute=120)
    start_urls = ("https://sklepimpuls.pl/sklep-piwo-kraftowe/listing/product_cat-piwo/",)

    def parse_product(self, page: Page) -> ShopProduct:
        soup = page.soup
        # Product pages are Divi layouts; `.et_pb_wc_*` modules hold the main product, while
        # bare WooCommerce selectors would also match the related-products carousel.
        regular_price, discount_price = self.prices(soup.select_one(".et_pb_wc_price .price"))
        attributes = self.attributes(soup)
        product = self.json_ld_product(soup)
        offer = _first(product.get("offers"))
        # JSON-LD prices exclude VAT, so prices come from the page and only the rest from here.
        return ShopProduct(
            url=page.url,
            shop=self.name,
            name=require(text_of(soup.select_one(".et_pb_wc_title h1")), "product title"),
            regular_price=require(regular_price, "price"),
            discount_price=discount_price,
            currency=offer.get("priceCurrency", "PLN"),
            in_stock=self.availability(offer),
            styles=split_list(attributes.get("styl")),
            abv=parse_float(attributes.get("alkohol")),
            extract=parse_float(attributes.get("ekstrakt")),
            volume_ml=parse_volume_ml(attributes.get("pojemność")),
            ean=attributes.get("ean") or product.get("sku"),
            image_url=_first(product.get("image")).get("url"),
            description=paragraphs_of(soup.select_one(".et_pb_wc_description")),
        )


def _first(value: Any) -> dict[str, Any]:
    """JSON-LD allows one object or a list of them; normalize to the first object."""
    if isinstance(value, list):
        value = value[0] if value else None
    return value if isinstance(value, dict) else {}
