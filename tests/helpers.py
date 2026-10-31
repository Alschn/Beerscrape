import json
import os
from pathlib import Path

import pytest

from beerscrape.core import Page
from beerscrape.models import Item

FIXTURES = Path(__file__).parent / "fixtures"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"


def fixture_path(*parts: str) -> Path:
    return FIXTURES.joinpath(*parts)


def fixture_page(url: str, label: str, *parts: str) -> Page:
    return Page(url=url, html=fixture_path(*parts).read_bytes(), label=label)


def assert_matches_golden(item: Item, *parts: str) -> None:
    """Compare an item with a reviewed JSON snapshot. Run with UPDATE_GOLDEN=1 to rewrite it."""
    path = fixture_path(*parts)
    actual = item.model_dump(mode="json")
    if UPDATE_GOLDEN or not path.exists():
        path.write_text(json.dumps(actual, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if not UPDATE_GOLDEN:
            pytest.fail(f"golden file {path.name} was missing and has been written; review it")
    assert actual == json.loads(path.read_text(encoding="utf-8"))
