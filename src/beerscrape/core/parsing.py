"""Small, pure helpers for turning scraped Polish-language text into values."""

import re
import unicodedata
from decimal import Decimal

from bs4.element import PageElement, Tag

_WHITESPACE = re.compile(r"\s+")
_NUMBER = re.compile(r"\d+(?:[.,]\d+)?")
_RANGE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:[-–—]\s*(\d+(?:[.,]\d+)?)|(\+))?")
_VOLUME = re.compile(r"(\d+(?:[.,]\d+)?)\s*(ml|cl|l)\b", re.IGNORECASE)
_VOLUME_FACTORS = {"ml": 1, "cl": 10, "l": 1000}
_SENTENCE_END = (".", "!", "?", ":", ";")
_LIST_BULLETS = ("-", "•", "*", "–")


class ParseError(ValueError):
    """A page lacks something its parser cannot do without."""


def require[T](value: T | None, what: str) -> T:
    if value is None:
        raise ParseError(f"missing {what}")
    return value


def clean_text(text: str) -> str:
    """Normalize unicode (NFKC keeps Polish letters composed) and collapse whitespace."""
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFKC", text)).strip()


def text_of(node: PageElement | None, separator: str = " ") -> str | None:
    """Cleaned text of a node, or None when the node is missing or empty."""
    if node is None:
        return None
    text = node.get_text(separator) if isinstance(node, Tag) else str(node)
    return clean_text(text) or None


def paragraphs_of(node: Tag | None) -> str | None:
    """Cleaned text of a node with one line per block of text, or None when empty."""
    if node is None:
        return None
    lines = (clean_text(line) for line in node.get_text("\n").split("\n"))
    return "\n".join(line for line in lines if line) or None


def attr_of(node: Tag | None, name: str) -> str | None:
    """A single-valued attribute of a node, or None."""
    if node is None:
        return None
    value = node.get(name)
    return value.strip() or None if isinstance(value, str) else None


def _to_float(number: str) -> float:
    return float(number.replace(",", "."))


def parse_float(text: str | None) -> float | None:
    """First number in the text, accepting a decimal comma: '5,2 %' -> 5.2."""
    if not text or not (match := _NUMBER.search(text)):
        return None
    return _to_float(match.group())


def parse_price(text: str | None) -> Decimal | None:
    """A price such as '1 234,56 zł' -> Decimal('1234.56')."""
    if not text:
        return None
    compact = re.sub(r"(?<=\d)[\s.](?=\d{3}\b)", "", clean_text(text))
    if not (match := _NUMBER.search(compact)):
        return None
    return Decimal(match.group().replace(",", "."))


def parse_range(text: str | None) -> tuple[float, float | None] | None:
    """'12-15° Blg' -> (12.0, 15.0); '15+' -> (15.0, None); '4,5' -> (4.5, 4.5)."""
    if not text or not (match := _RANGE.search(text)):
        return None
    low, high, open_ended = match.groups()
    if open_ended:
        return _to_float(low), None
    return _to_float(low), _to_float(high or low)


def parse_volume_ml(text: str | None) -> float | None:
    """'Puszka 500 ML' -> 500.0; '0,5L' -> 500.0; '33 cl' -> 330.0."""
    if not text or not (match := _VOLUME.search(text)):
        return None
    amount, unit = match.groups()
    return round(_to_float(amount) * _VOLUME_FACTORS[unit.lower()], 3)


def split_list(text: str | None, separator: str = ",") -> list[str]:
    """'American Pilsner, Pils' -> ['American Pilsner', 'Pils']."""
    if not text:
        return []
    return [part for part in (clean_text(p) for p in text.split(separator)) if part]


def unwrap_lines(lines: list[str]) -> str:
    """Re-join text that was hard-wrapped (e.g. pasted from a PDF), keeping real line breaks.

    A line is merged into the previous one when the previous line does not end a sentence
    and the line itself does not start with an uppercase letter (so lowercase words, digits
    and brackets continue the sentence). Bullet points always start a new line.
    """
    merged: list[str] = []
    for raw in lines:
        line = clean_text(raw)
        if not line:
            continue
        if (
            merged
            and not merged[-1].endswith(_SENTENCE_END)
            and not line.startswith(_LIST_BULLETS)
            and not line[0].isupper()
        ):
            merged[-1] = f"{merged[-1]} {line}"
        else:
            merged.append(line)
    return "\n".join(merged)
