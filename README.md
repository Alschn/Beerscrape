# Beerscrape

Scrapers for craft beer shops and brewing knowledge sources, built on
[Crawlee](https://crawlee.dev/python/), BeautifulSoup and Pydantic.

## Supported sites

| Name           | Source                                                     | Items         |
|----------------|------------------------------------------------------------|---------------|
| `sklep-impuls` | https://sklepimpuls.pl (WooCommerce)                       | `ShopProduct` |
| `piwoteka`     | https://sklep.piwoteka.pl (Ebexo)                          | `ShopProduct` |
| `pspd-styles`  | https://pspd.org.pl/style-piwa (successor of kompendiumpiwa.pl) | `BeerStyle` |

## Usage

Requires Python 3.14 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run beerscrape list
uv run beerscrape run piwoteka                     # writes results/piwoteka.json
uv run beerscrape run sklep-impuls --limit 50 -o sample.json
```

Output is a JSON document with `site`, `scraped_at`, `count` and `items` (sorted by URL).
The item schemas live in [`src/beerscrape/models`](src/beerscrape/models).

## Architecture

```
src/beerscrape/
  core/
    page.py            Page, Link, PageResult: what sites receive and return
    site.py            Site: start links + a parser per page label; CrawlSettings
    engine.py          Engine protocol, CrawlResult, Failure
    crawlee_engine.py  the production Engine (queueing, retries, rate limiting)
    parsing.py         pure text helpers (Polish decimals, ranges, volumes, ...)
    export.py          JSON exporter
  models/              Pydantic models: ShopProduct, BeerStyle
  sites/
    shops/             ShopSite (listing -> product), WooCommerceShop, concrete shops
    brewing/           style guides and other brewing material
  registry.py          site name -> class, used by the CLI
```

Sites are pure: they turn a `Page` (URL + HTML) into items and links to follow and never do
I/O. Engines do the fetching. This keeps all parsing testable offline:

- `tests/unit`: parsing helpers, models, WooCommerce helpers
- `tests/sites`: each site against saved HTML in `tests/fixtures`, including golden JSON
  snapshots (`*.expected.json`) and a full crawl on the in-memory `FixtureEngine`
- `tests/engine`: `FixtureEngine` and `CrawleeEngine` (against a local HTTP server) share
  the same crawl semantics
- `tests/live`: smoke tests against the real sites, run weekly in CI

### Adding a site

1. Save a listing page and a product page under `tests/fixtures/<site>/`.
2. Subclass `ShopSite` (or `WooCommerceShop`) and implement `parse_listing` /
   `parse_product`; for other content subclass `Site` directly.
3. Register it in `registry.py` and add tests in `tests/sites/`.

## Development

```bash
uv run pytest                  # offline tests
uv run pytest -m live          # hit the real websites
UPDATE_GOLDEN=1 uv run pytest  # rewrite golden snapshots, then review the diff
uv run ruff format src tests && uv run ruff check src tests
uv run pyright
```
