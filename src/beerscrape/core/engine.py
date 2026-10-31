from dataclasses import dataclass, field
from typing import Protocol

from beerscrape.core.site import Site
from beerscrape.models import Item


@dataclass(frozen=True, slots=True)
class Failure:
    url: str
    label: str
    error: str


@dataclass(slots=True)
class CrawlResult[T: Item]:
    items: list[T] = field(default_factory=list)
    failures: list[Failure] = field(default_factory=list)


class Engine(Protocol):
    """Runs a site: fetches its pages, feeds them to its handlers and follows returned links.

    Every engine must visit each URL at most once and record (not raise) per-page failures.
    """

    async def run[T: Item](self, site: Site[T]) -> CrawlResult[T]: ...
