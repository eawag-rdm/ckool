from ckool.datacite.doi_generator import generate_doi, revert_doi


def test_generate_doi():
    assert generate_doi("abc", 293, 0) == "abc/000AJ5"


def test_revert_doi():
    assert revert_doi("abc/000AJ5") == {"prefix": "abc", "intid": 293, "offset": 0}
