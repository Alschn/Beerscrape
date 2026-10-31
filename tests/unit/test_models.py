from decimal import Decimal

import pytest
from pydantic import ValidationError

from beerscrape.models import Range, ShopProduct


def product(**overrides) -> ShopProduct:
    fields = {
        "url": "https://shop.test/p",
        "shop": "test",
        "name": "Porter",
        "regular_price": Decimal("10.00"),
    } | overrides
    return ShopProduct.model_validate(fields)


def test_prices_serialize_as_json_numbers():
    dumped = product(discount_price=Decimal("8.50")).model_dump(mode="json")
    assert dumped["regular_price"] == 10.0
    assert dumped["discount_price"] == 8.5


@pytest.mark.parametrize(
    "overrides",
    [
        {"name": ""},
        {"url": "/relative"},
        {"regular_price": Decimal("-1")},
        {"discount_price": Decimal("12.00")},
        {"abv": 140},
        {"unexpected": 1},
    ],
)
def test_invalid_products_are_rejected(overrides):
    with pytest.raises(ValidationError):
        product(**overrides)


def test_range_must_be_ordered_and_may_be_open_ended():
    assert Range(min=15).max is None
    with pytest.raises(ValidationError):
        Range(min=5, max=4)
