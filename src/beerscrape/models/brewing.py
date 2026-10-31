from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from beerscrape.models.base import Item


class Range(BaseModel):
    """An inclusive numeric range; `max` is None for open-ended values such as '15+'."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    min: float = Field(ge=0)
    max: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.max is not None and self.max < self.min:
            raise ValueError(f"range max {self.max} is below min {self.min}")
        return self


class ChangelogEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    changed_on: date | None
    author: str | None
    description: str


class BeerStyle(Item):
    """A beer style description from a style guide."""

    source: str
    name: str = Field(min_length=1)
    extract_blg: Range | None = None
    abv: Range | None = None
    ibu: Range | None = None
    color_ebc: Range | None = None
    description: str = Field(description="Full description text, line breaks preserved")
    sections: dict[str, str] = Field(
        default_factory=dict,
        description="Description split by headings, when the page marks headings up",
    )
    changelog: list[ChangelogEntry] = Field(default_factory=list)
