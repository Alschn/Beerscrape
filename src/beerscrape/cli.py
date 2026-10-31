import asyncio
import logging
import time
from pathlib import Path
from typing import Annotated

import typer

from beerscrape.core.crawlee_engine import CrawleeEngine
from beerscrape.core.export import JsonExporter
from beerscrape.registry import SITES, get_site

app = typer.Typer(
    help="Scrape craft beer shops and brewing knowledge sources.", no_args_is_help=True
)


@app.command("list")
def list_sites() -> None:
    """Show the sites that can be scraped."""
    for name, site in sorted(SITES.items()):
        summary = next(iter((site.__doc__ or "").strip().splitlines()), "")
        typer.echo(f"{name:<16} {summary}")


@app.command()
def run(
    site_name: Annotated[str, typer.Argument(metavar="SITE", help="Site name, see `list`.")],
    output: Annotated[
        Path | None, typer.Option("--output", "-o", help="Defaults to results/<site>.json")
    ] = None,
    limit: Annotated[
        int | None, typer.Option(help="Stop after roughly this many requests (quick checks).")
    ] = None,
) -> None:
    """Crawl a site and write its items to a JSON file."""
    logging.basicConfig(
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", level=logging.INFO
    )
    try:
        site = get_site(site_name)
    except KeyError as error:
        raise typer.BadParameter(str(error.args[0]), param_hint="SITE") from None

    started = time.perf_counter()
    result = asyncio.run(CrawleeEngine(max_requests=limit).run(site))
    path = output or Path("results") / f"{site.name}.json"
    JsonExporter().export(site.name, result.items, path)

    typer.echo(
        f"{len(result.items)} items from {site.name} written to {path} "
        f"in {time.perf_counter() - started:.1f}s"
    )
    if result.failures:
        typer.echo(f"{len(result.failures)} pages failed:", err=True)
        for failure in result.failures[:20]:
            typer.echo(f"  [{failure.label}] {failure.url}: {failure.error}", err=True)
        raise typer.Exit(code=1)
