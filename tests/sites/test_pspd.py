import pytest
from fakes import FixtureEngine
from helpers import assert_matches_golden, fixture_page, fixture_path

from beerscrape.models import BeerStyle, Range
from beerscrape.sites.brewing.pspd import PspdStyleGuide

site = PspdStyleGuide()
LIST_URL = "https://pspd.org.pl/style-piwa"


def style(slug: str) -> BeerStyle:
    page = fixture_page(f"{LIST_URL}/{slug}", site.STYLE, "pspd", f"{slug}.html")
    [item] = site.handle(page).items
    return item


def test_style_list_links_every_style_once():
    page = fixture_page(LIST_URL, site.STYLE_LIST, "pspd", "style-list.html")
    links = site.handle(page).links

    assert len(links) == 127
    assert len({link.url for link in links}) == 127
    assert links[0].url == f"{LIST_URL}/albae-cerevisiae"
    assert {link.label for link in links} == {site.STYLE}


@pytest.mark.parametrize(
    "slug",
    [
        "irish-stout",
        "albae-cerevisiae",
        "american-ipa",
        "lager-wiedenski",
        "niskoalkoholowy-lichtenhainer",
    ],
)
def test_style_matches_golden(slug):
    assert_matches_golden(style(slug), "pspd", f"{slug}.expected.json")


def test_parameters_come_from_the_sidebar_and_unset_zero_is_none():
    stout = style("irish-stout")
    assert stout.extract_blg == Range(min=9, max=11)
    assert stout.color_ebc == Range(min=100.5, max=157.6)
    assert style("albae-cerevisiae").color_ebc is None  # sidebar shows "0.0EBC"


def test_sections_are_split_by_marked_up_headings():
    stout = style("irish-stout")
    assert list(stout.sections) == [
        "appearance",
        "aroma",
        "taste",
        "bitterness",
        "mouthfeel",
        "comment",
        "ingredients_and_technology",
    ]
    # Hard-wrapped PDF lines are joined, bullet lists keep their line breaks.
    assert stout.sections["appearance"].startswith(
        "Barwa od ciemnobrązowej do czarnej, zwykle nieprzejrzyste. Średnio obfita, trwała, drobno "
        "pęcherzykowa"
    )
    assert (
        "\n- Ca (wapń) – 50-75 ppm\n- Mg (magnez)" in stout.sections["ingredients_and_technology"]
    )


def test_style_title_and_changelog_footer_are_not_part_of_the_description():
    albae = style("albae-cerevisiae")
    assert not albae.description.startswith("ALBAE")
    assert "2019-02-26" not in albae.description
    assert [entry.author for entry in albae.changelog] == [
        "Magdalena Bergmann",
        "Michał Krzysztofik, FIlip Mazur",
        "Łukasz Wróblewski",
    ]


def test_plain_text_description_is_split_on_headings_alone_on_a_line():
    ipa = style("american-ipa")
    assert not ipa.description.startswith("American IPA")
    assert list(ipa.sections)[:3] == ["parameters", "appearance", "aroma"]
    assert ipa.sections["appearance"].startswith("Barwa od złotego do jasnego bursztynu.")


def test_markdown_backslashes_are_stripped_from_headings_and_lines():
    lager = style("lager-wiedenski")
    assert list(lager.sections)[:4] == ["appearance", "aroma", "taste", "bitterness"]
    assert "\\" not in lager.description


def test_bold_hard_wrapped_note_is_text_not_headings():
    lichtenhainer = style("niskoalkoholowy-lichtenhainer")
    assert all(not key.startswith(("*UWAGA", "jedna", "piwowar")) for key in lichtenhainer.sections)
    assert "*UWAGA! W tej kategorii" in lichtenhainer.description


async def test_crawl_visits_list_then_every_style():
    engine = FixtureEngine(
        {LIST_URL: fixture_path("pspd", "style-list.html")},
        fallbacks={site.STYLE: fixture_path("pspd", "irish-stout.html")},
    )
    result = await engine.run(site)

    assert result.failures == []
    assert len(result.items) == 127
