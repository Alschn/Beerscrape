from decimal import Decimal

import pytest
from fakes import FixtureEngine
from helpers import assert_matches_golden, fixture_page, fixture_path

from beerscrape.core.parsing import ParseError
from beerscrape.models import ShopProduct
from beerscrape.sites.shops.sklep_impuls import SklepImpuls

site = SklepImpuls()
START_URL = site.start_urls[0]
PRODUCT_URL = "https://sklepimpuls.pl/produkt/fixture/"


def product(fixture: str) -> ShopProduct:
    page = fixture_page(PRODUCT_URL, site.PRODUCT, "sklep_impuls", f"{fixture}.html")
    [item] = site.handle(page).items
    return item


def test_listing_links_products_and_next_page():
    page = fixture_page(START_URL, site.LISTING, "sklep_impuls", "listing_page_1.html")
    links = site.handle(page).links

    products = [link.url for link in links if link.label == site.PRODUCT]
    assert len(products) == 30
    assert all(url.startswith("https://sklepimpuls.pl/produkt/") for url in products)
    assert [link.url for link in links if link.label == site.LISTING] == [
        f"{START_URL}?product-page=2"
    ]


def test_last_listing_page_has_no_next_page():
    page = fixture_page(START_URL, site.LISTING, "sklep_impuls", "listing_page_last.html")
    links = site.handle(page).links

    assert {link.label for link in links} == {site.PRODUCT}
    assert len(links) == 22


@pytest.mark.parametrize("fixture", ["product_beer", "product_discounted", "product_glass"])
def test_product_matches_golden(fixture):
    assert_matches_golden(product(fixture), "sklep_impuls", f"{fixture}.expected.json")


def test_beer_attributes_and_gross_price():
    beer = product("product_beer")
    # The page shows the gross price; JSON-LD would give the net 12.59.
    assert beer.regular_price == Decimal("15.49")
    assert beer.discount_price is None
    assert (beer.abv, beer.volume_ml, beer.styles) == (5.5, 500.0, ["American Pilsner", "Pils"])
    assert beer.ean == "5906436218734"


def test_discount_is_read_from_struck_through_price():
    beer = product("product_discounted")
    assert (beer.regular_price, beer.discount_price) == (Decimal("16.69"), Decimal("13.49"))


def test_non_beer_products_have_no_beer_attributes():
    glass = product("product_glass")
    assert (glass.abv, glass.styles) == (None, [])


def test_page_without_product_title_is_a_parse_error():
    page = fixture_page(PRODUCT_URL, site.PRODUCT, "sklep_impuls", "listing_page_1.html")
    with pytest.raises(ParseError, match="product title"):
        site.handle(page)


async def test_crawl_follows_pagination_until_the_last_page():
    engine = FixtureEngine(
        {
            START_URL: fixture_path("sklep_impuls", "listing_page_1.html"),
            f"{START_URL}?product-page=2": fixture_path("sklep_impuls", "listing_page_last.html"),
        },
        fallbacks={site.PRODUCT: fixture_path("sklep_impuls", "product_beer.html")},
    )
    result = await engine.run(site)

    assert result.failures == []
    assert len(result.items) == 30 + 22
    assert [link.url for link in engine.visited if link.label == site.LISTING] == [
        START_URL,
        f"{START_URL}?product-page=2",
    ]
