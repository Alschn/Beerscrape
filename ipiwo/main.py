import asyncio
import logging
from pprint import pprint
import time
from typing import Any

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

import platform

SHOP_BASE_URL = 'https://ipiwo.pl/sklep/'
ACTIVE_DISCOUNTS_URL = 'https://ipiwo.pl/aktualne-promocje/'    # TODO

OptionalNode = Tag | NavigableString | None

def extract_links(a_tags: list[Tag]) -> list[str]:
    urls = [a_tag['href'] for a_tag in a_tags]
    return urls

def parse_next_page_link(a_tag: Tag | None) -> str:
    if not a_tag:
        return
    
    return a_tag['href']

def parse_beers_page(url: str, content):
    logging.info(f"Fetching beers from {url}")
    soup = BeautifulSoup(content, 'html.parser')

    a_tags = soup.find_all('a', class_='woocommerce-LoopProduct-link')
    next_url_a_tag = soup.find('a', class_='next page-number')

    next_page = parse_next_page_link(next_url_a_tag)
    links = extract_links(a_tags)
    return [links, next_page]

def parse_title(node: OptionalNode) -> str:
    print(node)

    if not node:
        return
    
    return node.text.strip()

def parse_active_discounts_page():
    # TODO: parse active discounts page
    pass

def parse_beer_page(url: str, content: str | bytes) -> dict[str, Any]:
    soup = BeautifulSoup(content, 'html.parser')

    # TODO: parse all the data

    return {
        'url': url,
        'name': '',
        'regular_price': '',
        'discount_price': '',
        'shop_categories': [],
        'description': '',
        'percentage': '',
        'extract': '',
        'volume_ml': '',
        'beer_categories': '',
    }

def request_beers_page(url: str) -> list[str]:
    try:
        response = requests.get(url)
        results = parse_beers_page(url, response.text)
        return results
    except Exception as e:
        logging.error(e, exc_info=True)

async def request_beer_details_page(url: str, session: ClientSession) -> dict[str, Any]:
    try:
        logging.info(f"Fetching beer from {url}")
        response = await session.get(url)
        content = await response.text()
        return parse_beer_page(url, content)
    except Exception as e:
        logging.error(e, exc_info=True)

async def run_pages_scraping(urls: list[str]) -> list[str]:
    async with ClientSession() as session:
        tasks = [request_beer_details_page(url, session) for url in urls]
        return await asyncio.gather(*tasks)

def collect_all_beers_links_through_pages(start_url: str) -> list[str]:
    urls = []
    current_page = start_url

    while current_page:
        current_urls, next_page_url = request_beers_page(current_page)
        urls.extend(current_urls)
        current_page = next_page_url

    return list(set(urls))

def main() -> None:
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-4s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


    start_time = time.perf_counter()

    beers_urls = collect_all_beers_links_through_pages(SHOP_BASE_URL)

    logging.info(f'All beers urls scraped in {time.perf_counter() - start_time:2f} seconds.')

    # pprint(beers_urls)


if __name__ == '__main__':
    main()
