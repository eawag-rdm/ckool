from ckool.utilities import meta_default


def test_make_meta_calculated(tmp_path):
    some_file = tmp_path / "test_file.txt"
    some_file.touch()

    assert meta_default(
        pkg_name="this-is-a-package",
        filename=some_file,
        hash_string="some-hash-string",
    ) == {
        "package_id": "this-is-a-package",
        "citation": "",
        "name": "test_file.txt",
        "resource_type": "Dataset",
        "url": "dummy",
        "restricted_level": "public",
        "hashtype": "sha256",
        "hash": "some-hash-string",
        "size": 0,
    }


def test_make_meta_provided(tmp_path):
    some_file = tmp_path / "test_file.txt"
    some_file.touch()

    assert meta_default(
        pkg_name="this-is-a-package",
        filename=some_file,
        hash_string="some-hash-string",
        resource_type="some",
        file_size=100,
    ) == {
        "package_id": "this-is-a-package",
        "citation": "",
        "name": "test_file.txt",
        "resource_type": "some",
        "url": "dummy",
        "restricted_level": "public",
        "hashtype": "sha256",
        "hash": "some-hash-string",
        "size": 100,
    }
