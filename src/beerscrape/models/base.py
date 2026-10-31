from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

type Price = Annotated[
    Decimal,
    Field(ge=0, max_digits=10, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]


class Item(BaseModel):
    """Anything a site produces. Every item is identified by the URL it was scraped from."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    url: str = Field(pattern=r"^https?://")
