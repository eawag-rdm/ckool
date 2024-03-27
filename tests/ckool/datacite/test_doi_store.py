import json

import pytest
from pytest_unordered import unordered

from ckool.datacite.doi_store import LocalDoiStore


def test_parse(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    assert lds.parse() == {
        "other": ["strange-file"],
        "name-2": {"package-url-2": ["file-2.pdf", "file-1.txt"]},
        "name-1": {
            "package-url-2": unordered(["file-2.pdf", "file-1.txt"]),
            "package-url-1": unordered(["file-2.pdf", "file-1.txt"]),
        },
    }


def test_write_exists_already(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    with pytest.raises(ValueError):
        lds.write(
            name="name-2",
            package="package-url-2",
            files=[
                tmp_path / "name-1" / "package-url-2" / "file-1.txt",
                tmp_path / "name-1" / "package-url-2" / "file-2.pdf",
            ],
        )
        lds.write(
            name="name-2",
            package="package-url-2",
            filename_content_map={
                "file-1.txt": "some_text",
                "file-2.pdf": "some_more_text",
            },
        )


def test_write_invalid(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    with pytest.raises(ValueError):
        lds.write(
            name="name-2",
            package="package-url-2",
            files=[
                tmp_path / "name-1" / "package-url-2" / "file-1.txt",
                tmp_path / "name-1" / "package-url-2" / "file-2.pdf",
            ],
            filename_content_map={
                "file-1.txt": "some_text",
                "file-2.pdf": "some_more_text",
            },
        )


def test_write_valid(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="name-23",
        package="package-url-232",
        filename_content_map={
            "file-1.txt": "some_text",
            "file-2.pdf": "some_more_text",
        },
    )
    assert (tmp_path / "name-23/package-url-232/file-1.txt").exists()
    assert (tmp_path / "name-23/package-url-232/file-2.pdf").exists()


def test_get_doi(tmp_path, local_structure_doi):
    _doi = "10.45934/25AZ53"
    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="person-2323",
        package="package-url-232",
        filename_content_map={
            "doi.txt": _doi,
        },
    )
    doi = lds.get_doi(
        package_name="package-url-232",
    )

    assert doi == _doi


def test_get_doi_not_found(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    with pytest.raises(FileNotFoundError):
        _ = lds.get_doi(
            package_name="package-url-232",
        )


def test_get_json_file_content(tmp_path, local_structure_doi):
    _json = {"name": "the affiliation"}
    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="person-2323",
        package="package-url-232",
        filename_content_map={
            "affiliation.json": json.dumps(_json),
        },
    )
    aff = lds.get_affiliations(
        package_name="package-url-232", filename="affiliation.json"
    )
    assert aff == _json
    orc = lds.get_orcids(package_name="package-url-232", filename="affiliation.json")
    assert orc == _json
    pub = lds.get_related_publications(
        package_name="package-url-232", filename="affiliation.json"
    )
    assert pub == _json


def test_get_json_file_content_not_found(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)

    orc = lds.get_orcids(package_name="package-url-232", filename="orcids.json")
    assert orc is None
