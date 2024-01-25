import pytest

from ckool.interfaces.dora import Dora


def test_dora_publication_link_dora_from_doi():
    assert Dora.publication_link_dora_from_doi("10.25678/00039Z") == (
        "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag%3A20376/datastream/PDF/"
        "Pomati-2020-Interacting_temperature%2C_nutrients_and_zooplankton-%28published_version%29.pdf"
    )


def test_dora_get_doi_from_dora_id():
    assert Dora.get_doi_from_dora_id("eawag:20376") == "10.3389/fmicb.2019.03155"
    assert Dora.get_doi_from_dora_id("eawag:19834") == "10.25678/0001HC"


def test_dora_doi_from_publication_link_doi_org_link():
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.25678/0001HC")
        == "10.25678/0001HC"
    )
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.1080/08941920.2018.1535102")
        == "10.1080/08941920.2018.1535102"
    )


def test_dora_doi_from_publication_link_dora_link():
    assert (
        Dora.doi_from_publication_link(
            "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:19834"
        )
        == "10.25678/0001HC"
    )
    assert (
        Dora.doi_from_publication_link(
            "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:1834"
        )
        == "10.1007/BF02486046"
    )


def test_dora_doi_from_publication_link_dora_link_not_found():
    with pytest.raises(ValueError):
        # invalid publication link matches only 3-6 number
        Dora.doi_from_publication_link(
            "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:02"
        )
        # No entries for dora id can be found
        Dora.doi_from_publication_link(
            "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:002"
        )
