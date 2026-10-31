from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import ClassVar

from beerscrape.core.page import Link, Page, PageResult
from beerscrape.models import Item

type Handler[T] = Callable[[Page], PageResult[T]]


@dataclass(frozen=True, slots=True)
class CrawlSettings:
    """How politely a site should be crawled. Engines translate this to their own options."""

    max_concurrency: int = 5
    max_requests_per_minute: float | None = 120
    max_retries: int = 3


class UnknownLabelError(LookupError):
    pass


class Site[T: Item](ABC):
    """Describes one website: where crawling starts and how each kind of page is parsed.

    Implementations are pure: they turn a `Page` into a `PageResult` and never do I/O,
    so they can be tested against saved HTML without a crawler or network.
    """

    name: ClassVar[str]
    settings: ClassVar[CrawlSettings] = CrawlSettings()

    @abstractmethod
    def start_links(self) -> Sequence[Link]: ...

    @abstractmethod
    def handlers(self) -> Mapping[str, Handler[T]]: ...

    def handle(self, page: Page) -> PageResult[T]:
        try:
            handler = self.handlers()[page.label]
        except KeyError:
            raise UnknownLabelError(
                f"{self.name} has no handler for label {page.label!r}"
            ) from None
        return handler(page)
