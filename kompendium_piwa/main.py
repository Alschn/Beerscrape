import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString, ResultSet

BEER_STYLES_BASE_URL = 'http://kompendiumpiwa.pl/style-piwa/'
RESULTS_JSON_PATH = Path(__file__).parent / 'results.json'

BLACK_DOT_UNICODE = 9899

OptionalNode = Tag | NavigableString | None


def extract_links(a_tags: list[Tag]) -> list[str]:
    urls = [a_tag['href'] for a_tag in a_tags]
    return urls


def generic_parse_next_sibling_text(node: OptionalNode) -> str | None:
    if not node:
        return

    text = node.next_sibling

    if not isinstance(text, NavigableString):
        return

    return str(text).strip()


def generic_parse_range(numbers_range_string: str, separator: str = "-") -> list[float]:
    numbers_separated = numbers_range_string.split(separator)
    numbers = [float(num.strip().replace(',', '.').replace('+', '')) for num in numbers_separated]
    return numbers


def parse_extract_range(node: OptionalNode) -> list[float]:
    if not (text := generic_parse_next_sibling_text(node)):
        return []

    numbers, _ = text.split('°')
    return generic_parse_range(numbers)


def parse_alcohol_volume_range(node: OptionalNode) -> list[float]:
    if not (text := generic_parse_next_sibling_text(node)):
        return []

    numbers, _ = text.split('%')
    return generic_parse_range(numbers)


def parse_bitterness_ibu_range(node: OptionalNode) -> list[float]:
    if not (text := generic_parse_next_sibling_text(node)):
        return []

    numbers, _ = text.split('IBU')
    return generic_parse_range(numbers)


def parse_color_ebc(node: OptionalNode) -> list[float]:
    if not (text := generic_parse_next_sibling_text(node)):
        return []

    numbers, _ = text.split('°')
    return generic_parse_range(numbers)


def parse_colors(rows: ResultSet) -> list[str]:
    if not rows:
        return []

    colors = [row['bgcolor'] for row in rows]
    return colors


def parse_temperature(node: OptionalNode) -> list[float]:
    if not (text := generic_parse_next_sibling_text(node)):
        return []

    numbers, _ = text.split('°')
    return generic_parse_range(numbers)


def parse_glass_type(node: OptionalNode) -> str | None:
    return generic_parse_next_sibling_text(node)


def parse_descriptive_text(node: OptionalNode) -> str | None:
    if not node or not node.parent:
        return

    heading = node.text.strip()
    text = str(node.parent.text)
    return text.replace(heading, '').strip()


def parse_description_section(node: OptionalNode) -> str | None:
    if not node:
        return

    return node.text


def parse_table(table_tag: Tag) -> dict[str, dict[str, list[int]]]:
    rows = table_tag.find_all('tr')
    tables = {}
    last_heading = ""

    for row in rows:
        text = row.text

        if not text.strip():
            continue

        if text.endswith("012345"):
            last_heading = text[:-6].lower()
            tables[last_heading] = {}

        else:
            key, dots = text[:-6], text[-6:]
            dict_key = key.lower()
            values = [index for index, dot in enumerate(dots) if ord(dot) == BLACK_DOT_UNICODE]
            tables[last_heading].update({dict_key: values})

    return tables


def retrieve_beer_style_urls(page_content: str | bytes) -> list[str]:
    soup = BeautifulSoup(page_content, 'html.parser')
    a_tags = soup.select('#main > h4 > a')
    return extract_links(a_tags)


def parse_beer_style_page(url: str, page_data: str) -> dict[str, Any]:
    soup = BeautifulSoup(page_data, 'html.parser')

    name = soup.select_one('#main article h2 > strong').text

    # beer parameters
    params_wrapper = soup.find('strong', text='Parametry:').parent

    extract_initial_range = parse_extract_range(params_wrapper.find('strong', text='Ekstrakt początkowy:'))
    extract_final_range = parse_extract_range(params_wrapper.find('strong', text='Ekstrakt końcowy:'))
    alcohol_volume_range = parse_alcohol_volume_range(params_wrapper.find('strong', text='Zawartość alkoholu:'))
    bitterness_range = parse_bitterness_ibu_range(params_wrapper.find('strong', text='Goryczka:'))
    color_ebc_range = parse_color_ebc(params_wrapper.find('strong', text='Barwa:'))
    colors_hex = parse_colors(params_wrapper.find_all('td', attrs={'bgcolor': True}))

    # descriptions
    description = parse_description_section(soup.select_one('table').find_next_sibling('div'))
    style_highlights = parse_descriptive_text(soup.find('strong', text='Wyróżniki stylu:'))
    history = parse_descriptive_text(soup.find('strong', text='Historia:'))
    aroma = parse_descriptive_text(soup.find('strong', text='Aromat:'))
    taste = parse_descriptive_text(soup.find('strong', text='Smak:'))
    bitterness = parse_descriptive_text(soup.find_all('strong', text='Goryczka:')[1])
    appearance = parse_descriptive_text(soup.find('strong', text='Wygląd:'))
    mouthfeel = parse_descriptive_text(soup.find('strong', text='Odczucie w ustach:'))
    resources_and_technology = parse_descriptive_text(soup.find('strong', text='Surowce i technologia:'))
    commercial_examples = parse_descriptive_text(soup.find('strong', text='Przykłady komercyjne:'))
    comment = parse_descriptive_text(soup.find('strong', text='Komentarz:'))
    temperature = parse_temperature(soup.find('i', attrs={'class': 'fa-thermometer-half'}))
    glass_type = parse_glass_type(soup.find('i', attrs={'class': 'fas fa-wine-glass-alt'}))

    # tables
    intensity_tables = parse_table(soup.find('table', attrs={'id': 'cssTable'}))

    return {
        'url': url,
        'name': name,
        'description': description,
        'extract_initial_range': extract_initial_range,
        'extract_final_range': extract_final_range,
        'alcohol_volume_range': alcohol_volume_range,
        'bitterness_range': bitterness_range,
        'color_ebc_range': color_ebc_range,
        'colors_hex': colors_hex,
        'style_highlights': style_highlights,
        'history': history,
        'aroma': aroma,
        'taste': taste,
        'bitterness': bitterness,
        'appearance': appearance,
        'mouthfeel': mouthfeel,
        'resources_and_technology': resources_and_technology,
        'commercial_examples': commercial_examples,
        'comment': comment,
        'temperature': temperature,
        'glass_type': glass_type,
        'intensity_table': intensity_tables,
    }


def request_beer_style_urls_page(url: str) -> list[str]:
    try:
        response = requests.get(url)
        return retrieve_beer_style_urls(response.content)
    except Exception as e:
        logging.error(e, exc_info=True)


async def request_beer_style_page(url: str, session: ClientSession) -> dict:
    try:
        response = await session.get(url)
        content = await response.text()
        return parse_beer_style_page(url, content)
    except Exception as e:
        logging.error(e, exc_info=True)


async def run_scraping(urls: list[str]) -> list[dict]:
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                request_beer_style_page(url, session)
            )
        return await asyncio.gather(*tasks)


def save_results_to_json(data: Any, path: str | Path, indent: int = 2) -> None:
    with open(path, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def main() -> None:
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-4s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info('Fetching beer styles urls...')

    start_time_urls = time.perf_counter()

    urls = request_beer_style_urls_page(BEER_STYLES_BASE_URL)

    logging.info(f'Beer styles urls scrapped in {time.perf_counter() - start_time_urls:.2f} seconds.')

    logging.info(f'Fetching beers pages...')

    start_time_scraping = time.perf_counter()

    data = asyncio.run(
        run_scraping(urls)
    )

    logging.info(f'Beers scrapping done in {time.perf_counter() - start_time_scraping:.2f} seconds.')

    non_empty_data = list(filter(bool, data))  # in case task fails and returns None
    non_empty_data.sort(key=lambda r: r['name'])  # because of asynchronous work data will be out of order

    logging.info(f'Writing results to json file...')

    save_results_to_json(non_empty_data, path=RESULTS_JSON_PATH)


if __name__ == '__main__':
    main()
