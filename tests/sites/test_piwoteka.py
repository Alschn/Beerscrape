from decimal import Decimal

from fakes import FixtureEngine
from helpers import assert_matches_golden, fixture_page, fixture_path

from beerscrape.core import Page
from beerscrape.models import ShopProduct
from beerscrape.sites.shops.piwoteka import Piwoteka

site = Piwoteka()
LISTING_URL = "https://sklep.piwoteka.pl/oferta/piwa"
PRODUCT_URL = "https://sklep.piwoteka.pl/porter-lodzki"


def parse_product(page: Page) -> ShopProduct:
    [item] = site.handle(page).items
    return item


def test_listing_resolves_product_links_against_the_site_root_despite_base_href():
    # The page sets <base href="https://sklep.piwoteka.pl/static/">; links start with "/".
    page = fixture_page(
        f"{LISTING_URL}?pp=1&rpp=15", site.LISTING, "piwoteka", "listing_page_1.html"
    )
    links = site.handle(page).links

    products = [link.url for link in links if link.label == site.PRODUCT]
    assert len(products) == 15
    assert products[0] == "https://sklep.piwoteka.pl/quadrupel-z-malinami-ba-collab-nowomiejski"
    assert [link.url for link in links if link.label == site.LISTING] == [
        f"{LISTING_URL}?pp=2&rpp=15"
    ]


def test_last_listing_page_has_no_next_page():
    page = fixture_page(
        f"{LISTING_URL}?pp=13&rpp=15", site.LISTING, "piwoteka", "listing_page_13.html"
    )
    assert [(link.url, link.label) for link in site.handle(page).links] == [
        ("https://sklep.piwoteka.pl/las-lagiewnicki", site.PRODUCT)
    ]


def test_product_matches_golden():
    product = parse_product(fixture_page(PRODUCT_URL, site.PRODUCT, "piwoteka", "product.html"))
    assert_matches_golden(product, "piwoteka", "product.expected.json")


def test_product_parameters_come_from_the_icon_grid():
    product = parse_product(fixture_page(PRODUCT_URL, site.PRODUCT, "piwoteka", "product.html"))
    assert product.brewery == "Browar Łódzki"
    assert (product.abv, product.extract, product.volume_ml) == (9.5, 22.0, 500.0)
    assert (product.regular_price, product.discount_price) == (Decimal("10.76"), None)


def test_old_price_turns_current_price_into_a_discount():
    # The recorded product is not on sale; fill its empty old-price slot to simulate one.
    html = fixture_path("piwoteka", "product.html").read_text(encoding="utf-8")
    on_sale = html.replace(
        '<price id="prOld"></price>', '<price id="prOld">12,99 <span>zł</span></price>'
    )
    assert on_sale != html

    product = parse_product(Page(PRODUCT_URL, on_sale, site.PRODUCT))
    assert (product.regular_price, product.discount_price) == (Decimal("12.99"), Decimal("10.76"))


async def test_crawl_follows_pagination_until_the_last_page():
    page_12 = f"{LISTING_URL}?pp=12&rpp=15"

    class FromPage12(Piwoteka):
        start_urls = (page_12,)

    engine = FixtureEngine(
        {
            page_12: fixture_path("piwoteka", "listing_page_12.html"),
            f"{LISTING_URL}?pp=13&rpp=15": fixture_path("piwoteka", "listing_page_13.html"),
        },
        fallbacks={site.PRODUCT: fixture_path("piwoteka", "product.html")},
    )
    result = await engine.run(FromPage12())

    assert result.failures == []
    assert len(result.items) == 15 + 1
