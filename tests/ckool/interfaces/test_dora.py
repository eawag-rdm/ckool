import pytest

from ckool.interfaces.dora import Dora


@pytest.mark.impure
def test_dora_publication_link_dora_from_doi_eawag():
    assert (
        Dora.publication_link_dora_from_doi("10.25678/00039Z")
        == "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:20376"
    )


@pytest.mark.impure
def test_dora_publication_link_dora_from_doi_other():
    res = Dora.publication_link_dora_from_doi("10.1002/9781119271659.ch1")
    assert res == "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:16556"


@pytest.mark.impure
def test_dora_get_doi_from_dora_id():
    assert Dora.get_doi_from_dora_id("eawag:20376") == "10.3389/fmicb.2019.03155"
    assert Dora.get_doi_from_dora_id("eawag:19834") == "10.25678/0001HC"


@pytest.mark.impure
def test_dora_doi_from_publication_link_doi_org_link():
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.25678/0001HC")
        == "10.25678/0001HC"
    )
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.1080/08941920.2018.1535102")
        == "10.1080/08941920.2018.1535102"
    )


@pytest.mark.impure
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


@pytest.mark.impure
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
