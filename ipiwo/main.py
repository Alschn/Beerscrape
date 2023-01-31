import asyncio
import json
import logging
import platform
import time
import unicodedata
from pathlib import Path
from typing import Any

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import NavigableString, ResultSet, Tag

SHOP_BASE_URL = 'https://ipiwo.pl/sklep/'
ACTIVE_DISCOUNTS_URL = 'https://ipiwo.pl/aktualne-promocje/'    # TODO later

RESULTS_JSON_PATH = Path(__file__).parent / 'results.json'

OptionalNode = Tag | NavigableString | None

def normalize_text(text: str) -> str:
    return unicodedata.normalize('NFKD', text)

def extract_links(a_tags: list[Tag]) -> list[str]:
    urls = [a_tag['href'] for a_tag in a_tags]
    return urls

def parse_next_page_link(a_tag: Tag | None) -> str:
    if not a_tag:
        return
    
    return a_tag['href']

def parse_beers_page(url: str, content):
    logging.info(f"Fetching beer links from {url}")
    soup = BeautifulSoup(content, 'html.parser')

    a_tags = soup.find_all('a', class_='woocommerce-LoopProduct-link')
    next_url_a_tag = soup.find('a', class_='next page-number')

    next_page = parse_next_page_link(next_url_a_tag)
    links = extract_links(a_tags)
    return [links, next_page]

def parse_text_node(node: OptionalNode) -> str | None:
    if not node:
        return
    
    return normalize_text(node.text.strip())

def text_to_number(text: str) -> str:
    return float(text.strip().replace(',', '.'))

def parse_number(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return

    return text_to_number(text)

def parse_price(node: OptionalNode) -> str | None:
    if not (text := parse_text_node(node)):
        return

    price, _ = text.split('zł')
    return text_to_number(price)

def parse_volume_ml(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)) or not text.endswith('l'):
        return
    
    text, _ = text.split('l')
    stripped_text = text.strip().replace(',', '.')
    return float(stripped_text) * 1000

def parse_price_nodes(nodes: ResultSet):
    regular_price_node = discount_price_node = None

    match len(nodes):
        case 1:
            regular_price_node = nodes[0]
        case 2:
            regular_price_node, discount_price_node = nodes

    return regular_price_node, discount_price_node

def parse_categories(node: OptionalNode) -> list[str]:
    if not node:
        return []

    return [
        {
            "category": category.text.strip(), 
            "href": category['href']
        } for category in node.select('a')
    ]

def parse_bundle(node: OptionalNode) -> list[str] | None:
    if not node:
        return []

    products = node.select('div.woosb-product')
    return [
        {
            'name': product.select_one('div.woosb-title a').text.strip(), 
            'href': product.select_one('div.woosb-thumb a')['href'], 
            'thumbnail': product.select_one('div.woosb-thumb img').attrs['data-src']
        } for product in products
    ]

def parse_beer_page(url: str, content: str | bytes) -> dict[str, Any]:
    soup = BeautifulSoup(content, 'html.parser')

    regular_price_node, discount_price_node = parse_price_nodes(soup.select('p.product-page-price span > bdi'))

    name = parse_text_node(soup.select_one('h1.product-title.entry-title'))
    regular_price = parse_price(regular_price_node)
    discount_price = parse_price(discount_price_node)
    shop_categories = parse_categories(soup.select_one('div.product_meta > span.posted_in'))
    description = parse_text_node(soup.select_one('#tab-description'))
    percentage = parse_number(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_alkohol > td'))
    extract = parse_number(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_ekstrakt > td'))
    volume_ml = parse_volume_ml(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_pojemnosc > td'))
    beer_categories = parse_categories(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_rodzaj > td'))
    bundle = parse_bundle(soup.select_one('div.woosb-bundled'))

    return {
        'url': url,
        'name': name,
        'regular_price': regular_price,
        'discount_price': discount_price,
        'shop_categories': shop_categories,
        'description': description,
        'percentage': percentage,
        'extract': extract,
        'volume_ml': volume_ml,
        'beer_categories': beer_categories,
        'bundle': bundle
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

def save_results_to_json(data: Any, path: str | Path, indent: int = 2) -> None:
    with open(path, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)

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

    start_time = time.perf_counter()

    beers = asyncio.run(
        run_pages_scraping(beers_urls)
    )

    logging.info(f'All beers scraped in {time.perf_counter() - start_time:2f} seconds.')

    non_empty_data = list(filter(bool, beers))      # in case task fails and returns None
    non_empty_data.sort(key=lambda r: r['name'])    # because of asynchronous work data will be out of order

    logging.info(f'Writing results to json file...')

    save_results_to_json(non_empty_data, path=RESULTS_JSON_PATH)

if __name__ == '__main__':
    main()
