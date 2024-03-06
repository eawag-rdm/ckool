import pytest

from ckool.interfaces.mixed_requests import fix_publication_link, get_citation_from_doi, search_orcid_by_author


@pytest.mark.impure
def test_get_citation_eawag_doi():
    eawag_doi = "10.25678/00039Z"
    assert (
        get_citation_from_doi(eawag_doi)
        == "Pomati, F., Shurin, J. B., Andersen, K. H., Tellenbach, C., &amp; Barton, A. D. (2020). Data for: Interacting temperature, nutrients and zooplankton grazing control phytoplankton size-abundance relationships in eight Swiss Lakes (Version 1.0) [Data set]. Eawag: Swiss Federal Institute of Aquatic Science and Technology. https://doi.org/10.25678/00039Z"
    )


@pytest.mark.impure
@pytest.mark.parametrize("doi", ["https://doi.org/10.25678/00039Z", "10.1109/5.771073"])
def test_get_citation_non_eawag_doi(doi):
    assert isinstance(get_citation_from_doi(doi), str)


@pytest.mark.dora
@pytest.mark.impure
def test_fix_publication_link_dora_escaped_colon():
    publication_link = "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag%3A16556"
    res = fix_publication_link(publication_link)
    assert res == {
        "publicationlink": "https://doi.org/10.1002/9781119271659.ch1",
        "publicationlink_dora": "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag%3A16556",
        "paper_doi": "10.1002/9781119271659.ch1",
    }


@pytest.mark.dora
@pytest.mark.impure
def test_fix_publication_link_dora_colon():
    publication_link = "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:16556"
    res = fix_publication_link(publication_link)
    assert res == {
        "publicationlink": "https://doi.org/10.1002/9781119271659.ch1",
        "publicationlink_dora": "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:16556",
        "paper_doi": "10.1002/9781119271659.ch1",
    }


@pytest.mark.dora
@pytest.mark.impure
def test_fix_publication_link_doi():
    publication_link = "https://dx.doi.org/10.1002/9781119271659.ch1"
    res = fix_publication_link(publication_link)
    assert res == {
        "publicationlink": "https://dx.doi.org/10.1002/9781119271659.ch1",
        "publicationlink_dora": "https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:16556",
        "paper_doi": "10.1002/9781119271659.ch1",
    }


def test_fix_publication_link_other():
    publication_link = "https://whatevar.url/this/is.txt"
    res = fix_publication_link(publication_link)
    assert res == {
        "publicationlink_url": publication_link,
        "publicationlink_dora": None,
        "paper_doi": None,
    }


def test_search_orcid_by_author():
    author = "Dennis, Stuart"
    assert search_orcid_by_author(author) == [
        {"id": "0000-0003-4263-3562", "url": "https://orcid.org/0000-0003-4263-3562"}
    ]


def test_search_orcid_by_author_does_not_exist():
    author = "dsdsdsd, sdasas"
    assert search_orcid_by_author(author) == []
