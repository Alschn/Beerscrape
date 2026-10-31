from decimal import Decimal

import pytest
from bs4 import BeautifulSoup

from beerscrape.core.parsing import (
    attr_of,
    clean_text,
    parse_float,
    parse_price,
    parse_range,
    parse_volume_ml,
    split_list,
    text_of,
    unwrap_lines,
)


def test_clean_text_collapses_whitespace_and_keeps_polish_letters_composed():
    assert clean_text("  Zawartość  alkoholu\n") == "Zawartość alkoholu"
    assert clean_text("ą") == "ą"


def test_text_of_returns_none_for_missing_or_blank_nodes():
    soup = BeautifulSoup("<p> a  <b>b</b> </p><i> </i>", "html.parser")
    assert text_of(soup.p) == "a b"
    assert text_of(soup.i) is None
    assert text_of(None) is None


def test_attr_of_ignores_multi_valued_and_missing_attributes():
    tag = BeautifulSoup('<a href=" /x " class="a b">', "html.parser").a
    assert attr_of(tag, "href") == "/x"
    assert attr_of(tag, "class") is None
    assert attr_of(tag, "title") is None
    assert attr_of(None, "href") is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [("5,2 %", 5.2), ("Alkohol 10.5%", 10.5), ("22 blg.", 22.0), ("brak", None), (None, None)],
)
def test_parse_float(text, expected):
    assert parse_float(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("15,49 zł", Decimal("15.49")),
        ("1 234,56 zł", Decimal("1234.56")),
        ("12.59", Decimal("12.59")),
        ("99,00 zł", Decimal("99.00")),
        ("", None),
        ("zł", None),
    ],
)
def test_parse_price(text, expected):
    assert parse_price(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12-15° Blg", (12.0, 15.0)),
        ("4.0-4.5%", (4.0, 4.5)),
        ("2,5 – 4,0", (2.5, 4.0)),
        ("15+ IBU", (15.0, None)),
        ("0.0EBC", (0.0, 0.0)),
        ("n/a", None),
    ],
)
def test_parse_range(text, expected):
    assert parse_range(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Puszka 500 ML", 500.0),
        ("Butelka 330 ML", 330.0),
        ("0,5L", 500.0),
        ("33 cl", 330.0),
        ("Szklanka 500ml", 500.0),
        ("brak", None),
    ],
)
def test_parse_volume_ml(text, expected):
    assert parse_volume_ml(text) == expected


def test_split_list_drops_empty_parts():
    assert split_list(" American Pilsner, Pils ,, ") == ["American Pilsner", "Pils"]
    assert split_list(None) == []


def test_unwrap_lines_joins_hard_wrapped_lines_but_keeps_sentences_and_bullets():
    lines = [
        "Barwa od ciemnej, zwykle nieprzejrzyste. Średnio obfita, drobno",
        "pęcherzykowa piana. Owocowość",
        "(jabłka, gruszki), stosunek co najmniej",
        "1:1 albo 2:1.",
        "Nowy akapit:",
        "- Ca (wapń) – 50-75 ppm",
        "- Mg (magnez) – 0-30 ppm",
        "",
        "Kolejne zdanie",
    ]
    assert unwrap_lines(lines) == (
        "Barwa od ciemnej, zwykle nieprzejrzyste. Średnio obfita, drobno pęcherzykowa piana. "
        "Owocowość (jabłka, gruszki), stosunek co najmniej 1:1 albo 2:1.\n"
        "Nowy akapit:\n"
        "- Ca (wapń) – 50-75 ppm\n"
        "- Mg (magnez) – 0-30 ppm\n"
        "Kolejne zdanie"
    )
