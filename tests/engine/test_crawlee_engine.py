"""CrawleeEngine against a local HTTP server: same semantics as FixtureEngine, real transport."""

from fakes import TINY_INDEX, TinySite
from pytest_httpserver import HTTPServer

from beerscrape.core.crawlee_engine import CrawleeEngine


def serve_tiny_site(httpserver: HTTPServer) -> str:
    html = {"Content-Type": "text/html; charset=utf-8"}
    httpserver.expect_request("/").respond_with_data(TINY_INDEX, headers=html)
    httpserver.expect_request("/a").respond_with_data("<h1>A</h1>", headers=html)
    httpserver.expect_request("/bad").respond_with_data("<p>oops</p>", headers=html)
    httpserver.expect_request("/b").respond_with_data("gone", status=404)
    return httpserver.url_for("").rstrip("/")


async def test_crawls_each_url_once_and_records_failures(httpserver: HTTPServer):
    base_url = serve_tiny_site(httpserver)
    result = await CrawleeEngine().run(TinySite(base_url))

    assert [(note.url, note.title) for note in result.items] == [(f"{base_url}/a", "A")]
    assert {(f.url, f.label) for f in result.failures} == {
        (f"{base_url}/b", "note"),
        (f"{base_url}/bad", "note"),
    }
    assert "no title" in next(f.error for f in result.failures if f.url.endswith("/bad"))
    requested = [request.path for request, _ in httpserver.log]
    assert sorted(requested) == ["/", "/a", "/b", "/bad"]


async def test_max_requests_limits_the_crawl(httpserver: HTTPServer):
    # The limit counts finished requests, so it is only exact while nothing else is in flight.
    base_url = serve_tiny_site(httpserver)
    result = await CrawleeEngine(max_requests=1).run(TinySite(base_url))

    assert [request.path for request, _ in httpserver.log] == ["/"]
    assert result.failures == []
