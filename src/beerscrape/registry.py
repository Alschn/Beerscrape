from typing import Any

from beerscrape.core import Site
from beerscrape.sites.brewing.pspd import PspdStyleGuide
from beerscrape.sites.shops.piwoteka import Piwoteka
from beerscrape.sites.shops.sklep_impuls import SklepImpuls

SITES: dict[str, type[Site[Any]]] = {
    site.name: site for site in (SklepImpuls, Piwoteka, PspdStyleGuide)
}


def get_site(name: str) -> Site[Any]:
    try:
        return SITES[name]()
    except KeyError:
        known = ", ".join(sorted(SITES))
        raise KeyError(f"unknown site {name!r}; known sites: {known}") from None
