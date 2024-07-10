import pathlib
from unittest.mock import Mock

import pytest

from ckool.datacite.parse_datacite_schema import SchemaParser
from ckool.other.prompt import (
    ask_for_affiliations,
    prompt_orcid,
    prompt_related_identifiers,
)


sp = SchemaParser(
    pathlib.Path(__file__).parent.parent.parent.parent
    / "src"
    / "ckool"
    / "datacite"
    / "schema"
    / "datacite"
    / "metadata_schema_4.5.xsd"
)


@pytest.mark.parametrize(
    "relationType", sp.get_schema_choices("relationType")
)
def test_prompt_related_identifiers_check_all_relation_types(relationType):
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "DOI",
        "Collection",
        relationType,
        "10.3389/fmicb.2019.03155",
    ]

    result = prompt_related_identifiers(prompt_func=mock_prompt)

    expected_result = {
        "relatedIdentifier": {
            "val": "10.3389/fmicb.2019.03155",
            "att": {
                "resourceTypeGeneral": "Collection",
                "relatedIdentifierType": "DOI",
                "relationType": relationType,
            },
        }
    }

    assert result == expected_result


@pytest.mark.parametrize(
    "doi", ["sdfsdfsfsd", "10.1016/j.abcdcddccdcdc.2024.121465 "]
)
def test_prompt_doi_exists_check_not_exists(doi):
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "DOI",
        "Collection",
        "IsSupplementTo",
        doi,
    ]
    try:
        _ = prompt_related_identifiers(prompt_func=mock_prompt)
    except Exception as e:
        assert isinstance(e, StopIteration)


def test_prompt_related_identifiers_valid_doi():
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "DOI",
        "Collection",
        "IsSupplementTo",
        "10.3389/fmicb.2019.03155",
    ]

    result = prompt_related_identifiers(prompt_func=mock_prompt)

    expected_result = {
        "relatedIdentifier": {
            "val": "10.3389/fmicb.2019.03155",
            "att": {
                "resourceTypeGeneral": "Collection",
                "relatedIdentifierType": "DOI",
                "relationType": "IsSupplementTo",
            },
        }
    }

    assert result == expected_result


def test_prompt_related_identifiers_invalid_doi(capfd):
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "DOI",
        "Collection",
        "IsSupplementTo",
        "this-is-not-valid",
        "cancel",
    ]

    prompt_related_identifiers(prompt_func=mock_prompt)
    out, err = capfd.readouterr()
    assert "could not be found. Please try again." in out


def test_prompt_related_identifiers_valid_url():
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "URL",
        "Collection",
        "IsSupplementTo",
        "https://opendata.eawag.ch",
    ]

    result = prompt_related_identifiers(prompt_func=mock_prompt)

    expected_result = {
        "relatedIdentifier": {
            "val": "https://opendata.eawag.ch",
            "att": {
                "resourceTypeGeneral": "Collection",
                "relatedIdentifierType": "URL",
                "relationType": "IsSupplementTo",
            },
        }
    }

    assert result == expected_result


def test_prompt_related_identifiers_invalid_url(capfd):
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "URL",
        "Collection",
        "IsSupplementTo",
        "this-is-not-valid",
        "cancel",
    ]

    prompt_related_identifiers(prompt_func=mock_prompt)
    out, err = capfd.readouterr()
    assert "could not be found. Please try again." in out


@pytest.mark.parametrize(
    "side_effects_to_cancel",
    [
        ("cancel",),
        ("ISBN", "cancel"),
        ("ISBN", "Collection", "cancel"),
        ("ISBN", "Collection", "IsSupplementTo", "cancel"),
    ],
)
def test_prompt_related_identifiers_cancelling(side_effects_to_cancel):
    mock_prompt = Mock()
    mock_prompt.side_effect = side_effects_to_cancel

    prompt_related_identifiers(prompt_func=mock_prompt)


def test_ask_for_single_affiliation():
    authors = ["Alice", "Bob"]
    mock_prompt = Mock()
    mock_prompt.side_effect = ["yes", "yes", "Opentech AI Lab"]

    result = ask_for_affiliations(authors, prompt_func=mock_prompt)

    expected_result = {author: "Opentech AI Lab" for author in authors}
    assert result == expected_result


def test_ask_for_individual_affiliations():
    authors = ["Alice", "Bob"]
    mock_prompt = Mock()
    mock_prompt.side_effect = ["yes", "no", "Opentech AI Lab", "Data Science Institute"]

    result = ask_for_affiliations(authors, prompt_func=mock_prompt)

    expected_result = {"Alice": "Opentech AI Lab", "Bob": "Data Science Institute"}
    assert result == expected_result


@pytest.mark.parametrize(
    "side_effects_to_skip,expected",
    [
        (("yes", "no", "skip", "Cal tech"), {"Bob": "Cal tech"}),
        (("yes", "no", "Cal tech", "skip"), {"Alice": "Cal tech"}),
    ],
)
def test_ask_for_individual_affiliations_skip(side_effects_to_skip, expected):
    authors = ["Alice", "Bob"]
    mock_prompt = Mock()

    mock_prompt.side_effect = side_effects_to_skip

    result = ask_for_affiliations(authors, prompt_func=mock_prompt)
    assert result == expected


def test_affiliations_not_provided():
    authors = ["Alice", "Bob"]
    mock_prompt = Mock()
    mock_prompt.side_effect = ["no"]

    ask_for_affiliations(authors, prompt_func=mock_prompt)


def test_prompt_orcid_valid(capfd):
    author = "last, first"
    mock_prompt = Mock()
    mock_prompt.side_effect = ["0000-0002-1825-0097"]

    result = prompt_orcid(author, prompt_func=mock_prompt)
    out, err = capfd.readouterr()
    assert result == "0000-0002-1825-0097"
    assert "DOI registered under" in out


def test_prompt_orcid_invalid_then_cancel(capfd):
    author = "Man, Bob"
    mock_prompt = Mock()
    mock_prompt.side_effect = ["not-valid-orcid", "cancel"]

    prompt_orcid(author, prompt_func=mock_prompt)
    out, err = capfd.readouterr()
    assert "The ORCiD could not be found. Please try again." in out


@pytest.mark.parametrize(
    "side_effects_to_cancel",
    [
        ("cancel",),
        ("0000-0002-1825-009X", "cancel"),  # Simulate an invalid ORCID then cancel
    ],
)
def test_prompt_orcid_cancelling(side_effects_to_cancel):
    author = "Lastname, Charlie"
    mock_prompt = Mock()
    mock_prompt.side_effect = side_effects_to_cancel

    prompt_orcid(author, prompt_func=mock_prompt)
