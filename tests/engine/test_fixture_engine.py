from fakes import TINY_INDEX, FixtureEngine, TinySite

PAGES = {
    "https://tiny.test/": TINY_INDEX,
    "https://tiny.test/a": "<h1>A</h1>",
    "https://tiny.test/bad": "<p>oops</p>",
}


async def test_crawls_each_url_once_and_records_failures():
    engine = FixtureEngine(PAGES)
    result = await engine.run(TinySite())

    assert [note.title for note in result.items] == ["A"]
    assert [link.url for link in engine.visited] == [
        "https://tiny.test/",
        "https://tiny.test/a",
        "https://tiny.test/b",
        "https://tiny.test/bad",
    ]
    assert {(f.url, f.error) for f in result.failures} == {
        ("https://tiny.test/b", "no fixture for URL"),
        ("https://tiny.test/bad", "ValueError('no title')"),
    }


async def test_fallback_only_serves_urls_without_their_own_fixture():
    engine = FixtureEngine(PAGES, fallbacks={"note": "<h1>Any</h1>"})
    result = await engine.run(TinySite())

    assert {(note.url, note.title) for note in result.items} == {
        ("https://tiny.test/a", "A"),
        ("https://tiny.test/b", "Any"),
    }
    assert [f.url for f in result.failures] == ["https://tiny.test/bad"]
