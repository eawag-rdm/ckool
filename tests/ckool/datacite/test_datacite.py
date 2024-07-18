import pytest

from ckool.datacite.datacite import DataCiteException


@pytest.mark.impure
def test_doi_list_via_client(datacite_instance):
    dois = datacite_instance.doi_list_via_client()
    # for data in dois:
    #     doi = data["id"]
    #     try:
    #         datacite_instance.doi_delete(doi)
    #     except Exception as e:
    #         print(e)
    # d = datacite_instance.doi_delete(doi)
    # assert datacite_instance.doi_list_via_client() == []


def test_generate_unused_dois(datacite_instance):
    dois = ['10.5524/000000', '10.5524/000011', '10.5524/000022', '10.5524/000033', '10.5524/000044', '10.5524/000055']
    generated = [doi for doi in datacite_instance._generate_unused_dois(
        dois, 2, datacite_instance.prefix, datacite_instance.offset
    )]
    assert generated == ['10.5524/000066', '10.5524/000077']

    dois = ['10.5524/000000', '10.5524/000033']
    generated = [doi for doi in datacite_instance._generate_unused_dois(
        dois, 1, datacite_instance.prefix, datacite_instance.offset
    )]
    assert generated == ['10.5524/000011']

    dois = ['10.5524/000000', '10.5524/000033']
    generated = [doi for doi in datacite_instance._generate_unused_dois(
        dois, 4, datacite_instance.prefix, datacite_instance.offset
    )]
    assert generated == ['10.5524/000011', '10.5524/000022', '10.5524/000044', '10.5524/000055']


@pytest.mark.impure
def test_doi_list_via_prefix(datacite_instance):
    dois = datacite_instance.doi_list_fast()
    print(dois)
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
