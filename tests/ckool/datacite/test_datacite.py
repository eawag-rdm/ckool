import pytest

from ckool.datacite.datacite import DataCiteException


@pytest.mark.impure
def test_doi_list_via_client(datacite_instance):
    datacite_instance.doi_list_via_client()
    _ = "10.5524/this-is-a-test-no-doi"
    # d = datacite_instance.doi_delete(doi)
    # assert datacite_instance.doi_list_via_client() == []


@pytest.mark.impure
def test_doi_list_via_prefix(datacite_instance):
    datacite_instance.doi_list_via_prefix()
    # assert datacite_instance.doi_list_via_prefix() == []


@pytest.mark.impure
def test_doi_reserve_and_delete(datacite_instance, load_env_file):
    doi = "10.5524/this-is-a-test-no-doi"
    d = datacite_instance.doi_reserve(doi)
    assert d
    d = datacite_instance.doi_delete(doi)
    assert d


@pytest.mark.impure
def test_doi_retrieve(datacite_instance, load_env_file):
    doi = "10.5524/this-is-a-test-no-doi"
    d = datacite_instance.doi_reserve(doi)
    assert d
    d = datacite_instance.doi_retrieve(doi)
    assert d.get("id") == doi
    d = datacite_instance.doi_delete(doi)
    assert d


@pytest.mark.impure
def test_doi_retrieve_not_available(datacite_instance, load_env_file):
    doi = "10.5524/this-doi-does-not-exist"
    with pytest.raises(DataCiteException):
        datacite_instance.doi_retrieve(doi)


@pytest.mark.impure
def test_doi_reserve_existing(datacite_instance, load_env_file):
    doi = "10.5524/this-is-a-test-no-doi"
    d = datacite_instance.doi_reserve(doi)
    assert d
    with pytest.raises(DataCiteException):
        datacite_instance.doi_reserve(doi)
    d = datacite_instance.doi_delete(doi)
    assert d


@pytest.mark.impure
def test_doi_delete_non_existing(datacite_instance, load_env_file):
    doi = "10.5524/this-is-a-test-no-doi"
    d = datacite_instance.doi_reserve(doi)
    assert d
    d = datacite_instance.doi_delete(doi)
    assert d
    with pytest.raises(DataCiteException):
        datacite_instance.doi_delete(doi)
