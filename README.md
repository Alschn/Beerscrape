# Beerscrape
A collection of python scripts to scrape information about craft beers from various websites.

## Supported websites
- *Kompendium Piwa:* https://kompendiumpiwa.pl/style-piwa/

## Tools, libraries, frameworks:

This setup has been tested with Python 3.10.

- `beautifulsoup4` - parsing html content
- `requests`, `aiohttp` + `asyncio` - sending (asynchronous) http requests
- `mkdocs-material` - documentation

### Setup

Install dependencies. Make sure you have `python` and `pipenv` installed globally.

```bash
pipenv install
```

Run scraping scripts

```bash
python ./kompendium_piwa/main.py
```

### Todos
- More supported websites
- Object oriented code instead of set of functions, code split into multiple files
- Documentation
- Unit tests
- CLI, main script in root directory
- CI/CD (unit tests + mkdocs deployment to GitHub Pages)
- Playwright support if some websites disallow web crawlers
