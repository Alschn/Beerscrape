from decimal import Decimal

from bs4 import BeautifulSoup

from beerscrape.sites.shops.woocommerce import WooCommerceShop


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def amount(value: str) -> str:
    return f'<span class="woocommerce-Price-amount amount"><bdi>{value} zł</bdi></span>'


def test_prices_regular_only():
    price = soup(f'<p class="price">{amount("15,49")}</p>').p
    assert WooCommerceShop.prices(price) == (Decimal("15.49"), None)


def test_prices_on_sale():
    price = soup(f'<p class="price"><del>{amount("16,69")}</del><ins>{amount("13,49")}</ins></p>').p
    assert WooCommerceShop.prices(price) == (Decimal("16.69"), Decimal("13.49"))


def test_prices_missing():
    assert WooCommerceShop.prices(None) == (None, None)


def test_json_ld_product_finds_product_in_graph_and_skips_broken_scripts():
    html = (
        '<script type="application/ld+json">{not json</script>'
        '<script type="application/ld+json">'
        '{"@graph": [{"@type": "WebSite"}, {"@type": "Product", "sku": "1"}]}'
        "</script>"
    )
    assert WooCommerceShop.json_ld_product(soup(html)) == {"@type": "Product", "sku": "1"}
    assert WooCommerceShop.json_ld_product(soup("<p></p>")) == {}


def test_availability():
    assert WooCommerceShop.availability({"availability": "http://schema.org/InStock"}) is True
    assert WooCommerceShop.availability({"availability": "https://schema.org/OutOfStock"}) is False
    assert WooCommerceShop.availability({}) is None
