import asyncio
import json
import logging
import platform
import random
import re
import time
from pathlib import Path
from typing import Any

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

BASE_URL = 'https://sklepimpuls.pl'
CRAFT_BEERS_BASE_URL = f'{BASE_URL}/sklep-piwo-kraftowe/?wpf=piwo_kraftowe&wpf_cols=5&wpf_browar=piwo&wpf_page=1'

OptionalNode = Tag | NavigableString | None

def extract_links(a_tags: list[Tag]) -> list[str]:
    urls = [a_tag['href'] for a_tag in a_tags]
    return urls

def parse_next_page_link(a_tag: OptionalNode) -> str | None:
    if not a_tag or isinstance(a_tag, NavigableString):
        return
    
    return a_tag['href']

def parse_beers_page(url: str, content):
    logging.info(f"Fetching beer links from {url}")
    soup = BeautifulSoup(content, 'html.parser')

    a_tags = soup.find_all('a', class_='woocommerce-LoopProduct-link')
    next_url_a_tag = soup.find('a', class_='next page-numbers')

    next_page = parse_next_page_link(next_url_a_tag)
    if next_page and not next_page.startswith(BASE_URL):
        next_page = f"{BASE_URL}{next_page}"

    links = extract_links(a_tags)
    return [links, next_page]

def request_beers_page(url: str) -> list[str] | None:
    try:
        response = requests.get(url)
        results = parse_beers_page(url, response.text)
        return results
    except Exception as e:
        logging.error(e, exc_info=True)

def parse_text_node(node: OptionalNode) -> str | None:
    if not node:
        return
    
    return node.text.strip()

def text_to_number(text: str) -> float:
    return float(text.strip().replace(',', '.'))

def parse_number(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return

    return text_to_number(text)

def parse_price(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return

    price, _ = text.split('zł')
    return text_to_number(price)

def parse_volume_ml(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return
    
    value = re.search(r'(\d+)', text)
    if not value:
        return
    
    return float(value.group(1))

def parse_extract(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return
    
    value = re.search(r'(\d+)', text)
    if not value:
        return
    
    return float(value.group(1))

def parse_percentage(node: OptionalNode) -> float | None:
    if not (text := parse_text_node(node)):
        return
    
    value = re.search(r'(\d+)', text)
    if not value:
        return
    
    return float(value.group(1))

def parse_beer_styles(node: OptionalNode) -> list[str] | None:
    if not (text := parse_text_node(node)):
        return
    
    return [style.strip() for style in text.split(',')]

def parse_brewery_url(node: OptionalNode) -> str | None:    
    if not node or isinstance(node, NavigableString):
        return
    
    return node['href']

def parse_beer_page(url: str, content: str | bytes) -> dict[str, Any]:
    soup = BeautifulSoup(content, 'html.parser')

    name = parse_text_node(soup.select_one('h1.product-title.entry-title'))
    regular_price = parse_price(soup.select_one('p.price span.woocommerce-Price-amount.amount'))
    discount_price = None
    description = parse_text_node(soup.select_one('div.sub-name.custom p'))
    extra_description = None    # todo
    percentage = parse_percentage(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_alkohol > td'))
    extract = parse_extract(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_ekstrakt > td'))
    volume_ml = parse_volume_ml(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_pojemnosc > td'))
    beer_style = parse_beer_styles(soup.select_one('tr.woocommerce-product-attributes-item--attribute_pa_styl > td'))
    bundle = None   # todo figure out what to return if it's a bundle
    brewery_url = parse_brewery_url(soup.select_one('div.et_pb_code_inner > p > a'))

    return {
        'url': url,
        'brewery_url': brewery_url,
        'image': '',
        'name': name,
        'regular_price': regular_price,
        'discount_price': discount_price,
        'description': description,
        'extra_description': extra_description,
        'percentage': percentage,
        'extract': extract,
        'volume_ml': volume_ml,
        'beer_styles': beer_style,
        'bundle': bundle,
    }

def collect_all_beers_links_through_pages(start_url: str) -> list[str]:
    urls = []
    current_page = start_url

    while current_page:
        result = request_beers_page(current_page)
        if not result:
            break
        current_urls, next_page_url = result
        urls.extend(current_urls)

        if next_page_url:
            random_sleep_time = random.random() + random.randint(0, 1)
            logging.info(f"Waiting for {random_sleep_time:2f} second... before fetching next page: {next_page_url}")
            time.sleep(random_sleep_time)

        current_page = next_page_url

    return list(set(urls))

def save_results_to_json(data: Any, path: str | Path, indent: int = 2) -> None:
    with open(path, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)

def request_beer_page(url: str) -> dict[str, Any] | None:
    try:
        logging.info(f"Fetching beer from {url}")
        response = requests.get(url)
        return parse_beer_page(url, response.text)
    except Exception as e:
        logging.error(e, exc_info=True)

async def request_beer_details_page(url: str, session: ClientSession) -> dict[str, Any] | None:
    random_sleep_time = random.random() + random.randint(0, 1)
    logging.info(f"Waiting for {random_sleep_time:2f} seconds... before fetching: {url}")
    await asyncio.sleep(random_sleep_time)

    try:
        logging.info(f"Fetching beer from {url}")
        response = await session.get(url)
        content = await response.text()
        return parse_beer_page(url, content)
    except Exception as e:
        logging.error(e, exc_info=True)

async def run_pages_scraping(urls: list[str]) -> list[dict[str, Any] | None]:
    async with ClientSession() as session:
        tasks = [
            request_beer_details_page(url, session) for url in urls
        ]
        return await asyncio.gather(*tasks)


def main() -> None:
    # from .links import beers_urls

    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-4s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    beers_urls = collect_all_beers_links_through_pages(CRAFT_BEERS_BASE_URL)

    start_time = time.perf_counter()

    beers = asyncio.run(
        run_pages_scraping(beers_urls)
    )

    beers = []
    for url in beers_urls:
        beer = request_beer_page(url)
        if beer:
            beers.append(beer)

    logging.info(f'All beers scraped in {time.perf_counter() - start_time:2f} seconds.')

    beers.sort(key=lambda r: r['name'])

    logging.info(f'Writing results to json file...')
    save_results_to_json(beers, path=Path(__file__).parent / 'beers.json')


if __name__ == '__main__':
    main()
