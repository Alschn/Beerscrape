import logging
import math
from uuid import uuid4

from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.http_clients import HttpClient, ImpitHttpClient
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

from beerscrape.core.engine import CrawlResult, Failure
from beerscrape.core.page import Link, Page
from beerscrape.core.site import Site
from beerscrape.models import Item

logger = logging.getLogger(__name__)


def _request(link: Link) -> Request:
    return Request.from_url(link.url, label=link.label)


class CrawleeEngine:
    """Production engine: Crawlee handles queueing, deduplication, retries and rate limits.

    Parse errors are recorded as failures without retrying; HTTP errors are retried by
    Crawlee and recorded once retries are exhausted.
    """

    def __init__(self, *, max_requests: int | None = None, http_client: HttpClient | None = None):
        self._max_requests = max_requests
        self._http_client = http_client

    async def run[T: Item](self, site: Site[T]) -> CrawlResult[T]:
        result = CrawlResult[T]()
        settings = site.settings
        storage_client = MemoryStorageClient()
        # Crawlee caches storages per process; a private queue keeps runs from seeing each
        # other's already-handled URLs.
        queue = await RequestQueue.open(alias=f"run-{uuid4().hex}", storage_client=storage_client)
        crawler = HttpCrawler(
            http_client=self._http_client or ImpitHttpClient(),
            storage_client=storage_client,
            request_manager=queue,
            max_requests_per_crawl=self._max_requests,
            max_request_retries=settings.max_retries,
            concurrency_settings=ConcurrencySettings(
                max_concurrency=settings.max_concurrency,
                desired_concurrency=settings.max_concurrency,
                max_tasks_per_minute=settings.max_requests_per_minute or math.inf,
            ),
        )

        @crawler.router.default_handler
        async def handle(context: HttpCrawlingContext) -> None:
            request = context.request
            label = request.label or ""
            # The requested URL, not the post-redirect one, so items keep the identity of the
            # links that led to them.
            page = Page(url=request.url, html=await context.http_response.read(), label=label)
            try:
                page_result = site.handle(page)
            except Exception as error:
                logger.exception("Failed to parse %s (%s)", request.url, label)
                result.failures.append(Failure(request.url, label, repr(error)))
                return
            result.items.extend(page_result.items)
            if page_result.links:
                await context.add_requests([_request(link) for link in page_result.links])

        @crawler.failed_request_handler
        async def failed(context: BasicCrawlingContext, error: Exception) -> None:
            request = context.request
            result.failures.append(Failure(request.url, request.label or "", repr(error)))

        try:
            await crawler.run([_request(link) for link in site.start_links()])
        finally:
            await queue.drop()
        return result
