"""Beer style guide of PSPD (Polskie Stowarzyszenie Piwowarów Domowych).

PSPD took over the style descriptions previously published on kompendiumpiwa.pl.
"""

import copy
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import ClassVar

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from beerscrape.core import CrawlSettings, Handler, Link, Page, PageResult, Site
from beerscrape.core.parsing import clean_text, parse_range, text_of, unwrap_lines
from beerscrape.models import BeerStyle, ChangelogEntry, Range

_STYLE_PATH = re.compile(r"^(?:https://pspd\.org\.pl)?(?:/pl)?/style-piwa/[^/?#]+$")
_FOOTER_MARKS = {"—", "–", "-"}

_MAX_HEADING_LENGTH = 40

SECTION_KEYS = {
    "parametry": "parameters",
    "podstawowe parametry": "parameters",
    "wygląd": "appearance",
    "aromat": "aroma",
    "smak": "taste",
    "goryczka": "bitterness",
    "odczucie w ustach": "mouthfeel",
    "odczucia w ustach": "mouthfeel",
    "ogólne wrażenie": "overall_impression",
    "komentarz": "comment",
    "uwagi": "notes",
    "historia": "history",
    "wyróżniki stylu": "style_highlights",
    "surowce i technologia": "ingredients_and_technology",
    "surowce": "ingredients_and_technology",
    "przykłady komercyjne": "commercial_examples",
}

PARAMETER_FIELDS = {
    "ekstrakt": "extract_blg",
    "alkohol": "abv",
    "ibu": "ibu",
    "barwa": "color_ebc",
}


class PspdStyleGuide(Site[BeerStyle]):
    """Beer style descriptions from pspd.org.pl/style-piwa."""

    name = "pspd-styles"
    settings = CrawlSettings(max_concurrency=3, max_requests_per_minute=60)

    BASE_URL: ClassVar[str] = "https://pspd.org.pl"
    STYLE_LIST: ClassVar[str] = "style-list"
    STYLE: ClassVar[str] = "style"

    def start_links(self) -> Sequence[Link]:
        return [Link(f"{self.BASE_URL}/style-piwa", self.STYLE_LIST)]

    def handlers(self) -> Mapping[str, Handler[BeerStyle]]:
        return {self.STYLE_LIST: self.parse_style_list, self.STYLE: self.parse_style}

    def parse_style_list(self, page: Page) -> PageResult[BeerStyle]:
        urls = dict.fromkeys(
            page.absolute(href)
            for a in page.soup.select("a[href]")
            if isinstance(href := a.get("href"), str) and _STYLE_PATH.match(href)
        )
        return PageResult(links=[Link(url, self.STYLE) for url in urls])

    def parse_style(self, page: Page) -> PageResult[BeerStyle]:
        soup = page.soup
        name = text_of(soup.select_one("main h1")) or ""
        description, sections = _parse_description(soup.select_one(".tiptap-content"), name)
        style = BeerStyle(
            url=page.url,
            source=self.name,
            name=name,
            description=description,
            sections=sections,
            changelog=_parse_changelog(_card(soup, "Historia zmian")),
            **_parse_parameters(_card(soup, "Parametry")),
        )
        return PageResult(items=[style])


def _card(soup: BeautifulSoup, heading: str) -> Tag | None:
    h2 = soup.find("h2", string=lambda s: s is not None and clean_text(s) == heading)
    return h2.parent if h2 else None


def _parse_parameters(card: Tag | None) -> dict[str, Range | None]:
    if card is None:
        return {}
    parameters: dict[str, Range | None] = {}
    for h3 in card.find_all("h3"):
        field = PARAMETER_FIELDS.get((text_of(h3) or "").casefold())
        value = h3.find_next_sibling()
        if field is None or not isinstance(value, Tag):
            continue
        bounds = parse_range(text_of(value.select_one("span")))
        # PSPD shows 0.0 for parameters that were never filled in.
        if bounds is None or bounds == (0.0, 0.0):
            parameters[field] = None
        else:
            parameters[field] = Range(min=bounds[0], max=bounds[1])
    return parameters


def _parse_changelog(card: Tag | None) -> list[ChangelogEntry]:
    if card is None:
        return []
    entries = []
    for summary in card.select("div.font-medium"):
        details = summary.find_next_sibling("div")
        spans = [text_of(span) for span in details.find_all("span")] if details else []
        author = spans[0] if spans else None
        entries.append(
            ChangelogEntry(
                changed_on=_parse_date(spans[1] if len(spans) > 1 else None),
                author=author,
                description=(text_of(summary) or "").lstrip("-–— "),
            )
        )
    return entries


def _parse_date(text: str | None) -> date | None:
    try:
        return datetime.strptime(text, "%d.%m.%Y").date() if text else None
    except ValueError:
        return None


def _parse_description(content: Tag | None, style_name: str) -> tuple[str, dict[str, str]]:
    """Return the full description and its sections.

    The content is free-form rich text: headings are either `<strong>` at the start of a
    paragraph or a known heading alone on a line, PDF-pasted pages put every line in its own
    `<p>`, and some pages are one text node. A paragraph with just a dash starts the changelog
    footer, which is dropped because the changelog card has the same data.
    """
    if content is None:
        return "", {}
    content = copy.copy(content)
    for br in content.find_all("br"):
        br.replace_with("\n")

    lines: list[str] = []
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None

    def start_section(heading: str) -> None:
        nonlocal current
        lines.append(heading)
        current = sections.setdefault(SECTION_KEYS.get(heading.casefold(), heading), [])

    for block in content.children:
        if isinstance(block, Tag):
            heading, text = _heading_of(block), block.get_text()
        elif isinstance(block, NavigableString):
            heading, text = None, str(block)
        else:
            continue

        if clean_text(text) in _FOOTER_MARKS:
            break
        if heading is not None:
            if heading.casefold() == style_name.casefold():
                continue
            start_section(heading)

        for raw_line in text.split("\n"):
            # Some descriptions were pasted from Markdown and end lines with a literal "\".
            line = raw_line.rstrip().removesuffix("\\")
            label = _normalize_heading(line)
            if not lines and label.casefold() == style_name.casefold():
                continue
            if label.casefold() in SECTION_KEYS:
                start_section(label)
                continue
            lines.append(line)
            if current is not None:
                current.append(line)

    return unwrap_lines(lines), {key: unwrap_lines(body) for key, body in sections.items()}


def _normalize_heading(text: str) -> str:
    return clean_text(text).rstrip(":\\ ").strip()


def _heading_of(paragraph: Tag) -> str | None:
    """Pop and return a leading `<strong>` heading, or None when the paragraph has none.

    Bold text counts as a heading when it is a known section name, or when it makes up the
    whole paragraph and looks like a title: short, not starting lowercase and not ending a
    sentence. Other bold paragraphs are emphasized notes (sometimes hard-wrapped), not headings.
    """
    first = next(
        (c for c in paragraph.children if not (isinstance(c, NavigableString) and not c.strip())),
        None,
    )
    if not isinstance(first, Tag) or first.name not in {"strong", "b"}:
        return None
    heading = _normalize_heading(first.get_text())
    rest = clean_text(paragraph.get_text().replace(first.get_text(), "", 1))
    if not heading or not (heading.casefold() in SECTION_KEYS or (not rest and _is_title(heading))):
        return None
    first.extract()
    return heading


def _is_title(text: str) -> bool:
    return (
        len(text) <= _MAX_HEADING_LENGTH
        and not text[0].islower()
        and not text.endswith((".", "!", "?", ","))
    )
