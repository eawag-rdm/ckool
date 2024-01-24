from ckool.interfaces.dora import Dora


def test_dora_doi_from_publication_link():
    # Dora.doi_from_publication_link("https://www.dora.lib4ri.ch/eawag/islandora/object/eawag%3A20376/datastream/PDF/Pomati-2020-Interacting_temperature%2C_nutrients_and_zooplankton-%28published_version%29.pdf")
    # Dora.doi_from_publication_link("https://www.dora.lib4ri.ch/eawag/islandora/object/eawag:19834")
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.25678/0001HC")
        == "10.25678/0001HC"
    )
    assert (
        Dora.doi_from_publication_link("https://doi.org/10.1080/08941920.2018.1535102")
        == "10.1080/08941920.2018.1535102"
    )


def test_dora_plink_dora_from_doi():
    print(Dora.publication_link_dora_from_doi("10.25678/00039Z"))


def test_dora_get_dora_record():
    pass
    #print(Dora._get_dora_record("10.25678/0002RE"))
