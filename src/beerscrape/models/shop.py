from typing import Self

from pydantic import Field, model_validator

from beerscrape.models.base import Item, Price


class ShopProduct(Item):
    """A product listed in an online shop. Beer-specific fields are None for other goods."""

    shop: str
    name: str = Field(min_length=1)
    regular_price: Price
    discount_price: Price | None = None
    currency: str = "PLN"
    in_stock: bool | None = None

    brewery: str | None = None
    styles: list[str] = Field(default_factory=list)
    abv: float | None = Field(default=None, ge=0, le=100, description="Alcohol by volume, %")
    extract: float | None = Field(default=None, ge=0, le=60, description="Original gravity, °Blg")
    volume_ml: float | None = Field(default=None, gt=0)

    ean: str | None = None
    image_url: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _discount_below_regular(self) -> Self:
        if self.discount_price is not None and self.discount_price >= self.regular_price:
            raise ValueError(
                f"discount_price {self.discount_price} is not below regular_price "
                f"{self.regular_price}"
            )
        return self
