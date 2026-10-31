import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from beerscrape.models import Item


class Exporter(Protocol):
    def export(self, site_name: str, items: Sequence[Item], path: Path) -> None: ...


class JsonExporter:
    """Writes items sorted by URL, wrapped in an envelope with run metadata."""

    def __init__(
        self, indent: int = 2, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self._indent = indent
        self._clock = clock

    def export(self, site_name: str, items: Sequence[Item], path: Path) -> None:
        ordered = sorted(items, key=lambda item: item.url)
        document = {
            "site": site_name,
            "scraped_at": self._clock().isoformat(timespec="seconds"),
            "count": len(ordered),
            "items": [item.model_dump(mode="json") for item in ordered],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(document, ensure_ascii=False, indent=self._indent) + "\n", encoding="utf-8"
        )
