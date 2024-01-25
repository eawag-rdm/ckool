import pytest

from ckool.interfaces.mixed_requests import get_citation_from_doi


def test_get_citation_eawag_doi():
    eawag_doi = "10.25678/00039Z"
    assert (
        get_citation_from_doi(eawag_doi)
        == "Pomati, F., Shurin, J. B., Andersen, K. H., Tellenbach, C., &amp; Barton, A. D. (2020). Data for: Interacting temperature, nutrients and zooplankton grazing control phytoplankton size-abundance relationships in eight Swiss Lakes (Version 1.0) [Data set]. Eawag: Swiss Federal Institute of Aquatic Science and Technology. https://doi.org/10.25678/00039Z"
    )


@pytest.mark.parametrize("doi", ["https://doi.org/10.25678/00039Z", "10.1109/5.771073"])
def test_get_citation_non_eawag_doi(doi):
    assert isinstance(get_citation_from_doi(doi), str)
