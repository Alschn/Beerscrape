from beerscrape.core import Page


def test_absolute_resolves_against_page_url():
    page = Page(url="https://shop.test/category/beers?page=2", html="<html></html>", label="x")
    assert page.absolute("/product/a") == "https://shop.test/product/a"
    assert page.absolute("b") == "https://shop.test/category/b"


def test_absolute_honours_base_href_and_drops_fragments():
    html = '<html><head><base href="https://shop.test/static/"></head></html>'
    page = Page(url="https://shop.test/offer", html=html, label="x")
    assert page.absolute("img/logo.png") == "https://shop.test/static/img/logo.png"
    assert page.absolute("/porter#reviews") == "https://shop.test/porter"


def test_soup_is_parsed_once():
    page = Page(url="https://shop.test/", html=b"<h1>Hi</h1>", label="x")
    assert page.soup is page.soup
