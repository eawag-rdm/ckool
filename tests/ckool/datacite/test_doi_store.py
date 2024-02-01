import pytest

from ckool.datacite.doi_store import LocalDoiStore


def test_parse(tmp_path, local_structure_doi):
    lds = LocalDoiStore(tmp_path)
    assert lds.parse() == {
        "other": ["strange-file"],
        "name-2": {"package-url-2": ["file-2.pdf", "file-1.txt"]},
        "name-1": {
            "package-url-2": ["file-2.pdf", "file-1.txt"],
            "package-url-1": ["file-2.pdf", "file-1.txt"],
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
    a = lds.write(
        name="name-23",
        package="package-url-232",
        filename_content_map={
            "file-1.txt": "some_text",
            "file-2.pdf": "some_more_text",
        },
    )
    assert (tmp_path / "name-23/package-url-232/file-1.txt").exists()
    assert (tmp_path / "name-23/package-url-232/file-2.pdf").exists()
