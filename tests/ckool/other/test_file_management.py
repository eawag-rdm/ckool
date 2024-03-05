import pytest

from ckool import TEMPORARY_DIRECTORY_NAME
from ckool.other.file_management import (
    find_archive,
    generate_archive_destination,
    iter_files,
    iter_package,
    match_via_include_exclude_patters,
    tar_files,
    zip_files,
)
from conftest import flatten_nested_structure


def test_match_via_include_exclude_patters():
    short_name = match_via_include_exclude_patters
    assert short_name("this/should/be/matched", None, None)
    assert short_name("this/should/be/matched", "this*", None)
    assert short_name("this/should/be/matched", None, "[0-9]")
    assert short_name("this/should/be/matched", "matched", "abc")

    assert not short_name("/no/match/2024", "abc", None)
    assert not short_name("/no/match/2024", None, "match")
    assert not short_name("/no/match/2024", "abc", "match")

    # conflicting patterns, exclude wins
    assert not short_name("/no/match/2024", "match", "match")


# def transform_pathlib_to_string(iterable: list | dict):
#     def is_dict_or_list(entity):
#         return isinstance(entity, dict) or isinstance(entity, list)
#
#     if isinstance(iterable, dict):
#         for key, val in iterable.items():
#             if is_dict_or_list(iterable[key]):
#                 transform_pathlib_to_string(iterable[key])
#             elif isinstance(iterable[key], pathlib.Path):
#                 iterable[key] = val.as_posix()
#     elif isinstance(iterable, list):
#         for idx, val in enumerate(iterable):
#             if is_dict_or_list(val):
#                 transform_pathlib_to_string(val)
#             elif isinstance(val, pathlib.Path):
#                 iterable[idx] = val.as_posix()
#
#     return iterable


def test_iter_files(tmp_path, my_package_dir):
    assert len([file.relative_to(tmp_path) for file in iter_files(tmp_path)]) == 5
    assert (
        len(
            [
                file.relative_to(tmp_path)
                for file in iter_files(tmp_path, include_pattern=r"\.py")
            ]
        )
        == 1
    )
    assert (
        len(
            [
                file.relative_to(tmp_path)
                for file in iter_files(tmp_path, include_pattern="/script")
            ]
        )
        == 1
    )


def test_iter_files_2(tmp_path):
    (tmp_path / "abc.json").touch()
    (tmp_path / "abc.bcd").touch()
    (tmp_path / "abc.json.sdd").touch()
    (tmp_path / "def.json").touch()
    (tmp_path / "ddd.json.json").touch()

    print(
        [
            file.relative_to(tmp_path)
            for file in iter_files(tmp_path, include_pattern=r".json$")
        ]
    )


def test_generate_tmp_file_paths(tmp_path, my_package_dir):
    assert generate_archive_destination(
        my_package_dir, tmp_path, tmp_dir_name=".ckool"
    ) == (my_package_dir.parent / ".ckool" / my_package_dir.name)


def test_zip_files(tmp_path, my_package_dir):
    archive_file = generate_archive_destination(
        my_package_dir, tmp_path, tmp_dir_name=".ckool"
    )

    file = zip_files(
        my_package_dir, archive_file, [file for file in iter_files(tmp_path)]
    )

    assert file == archive_file.with_suffix(".zip")


def test_tar_files(tmp_path, my_package_dir):
    archive_file = generate_archive_destination(
        my_package_dir, tmp_path, tmp_dir_name=".ckool"
    )

    file = tar_files(
        my_package_dir, archive_file, [file for file in iter_files(tmp_path)]
    )

    assert file == archive_file.with_suffix(".tar.gz")


def test_tar_file(tmp_path):
    (tmp_path / "some.txt").write_text("Test!")
    archive_file = generate_archive_destination(
        tmp_path, tmp_path, tmp_dir_name=".ckool"
    )

    assert tar_files(
        tmp_path,
        archive_file,
        [file for file in iter_files(tmp_path)],
        compression="xz",
    ).name.endswith(".tar.xz")
    assert tar_files(
        tmp_path,
        archive_file,
        [file for file in iter_files(tmp_path)],
        compression="bz2",
    ).name.endswith(".tar.bz2")
    assert tar_files(
        tmp_path,
        archive_file,
        [file for file in iter_files(tmp_path)],
        compression="gz",
    ).name.endswith(".tar.gz")


def test_iter_package_and_prepare_for_upload_prepare_all(tmp_path, my_package_dir):
    valid_results = [
        {"file": tmp_path / "my_data_package" / "readme.md", "folder": {}},
        {"file": tmp_path / "my_data_package" / "script.py", "folder": {}},
        {
            "file": "",
            "folder": {
                "location": tmp_path / "my_data_package" / "test_folder2",
                "root_folder": tmp_path / "my_data_package",
                "archive_destination": tmp_path
                / "my_data_package"
                / TEMPORARY_DIRECTORY_NAME
                / "test_folder2",
                "files": [
                    tmp_path / "my_data_package" / "test_folder2" / ".hidden",
                    tmp_path / "my_data_package" / "test_folder2" / "random",
                ],
            },
        },
        {
            "file": "",
            "folder": {
                "location": tmp_path / "my_data_package" / "test_folder1",
                "root_folder": tmp_path / "my_data_package",
                "archive_destination": tmp_path
                / "my_data_package"
                / TEMPORARY_DIRECTORY_NAME
                / "test_folder1",
                "files": [tmp_path / "my_data_package" / "test_folder1" / "text.txt"],
            },
        },
    ]
    valid_results = [sorted(flatten_nested_structure(entry)) for entry in valid_results]

    for result in iter_package(my_package_dir, ignore_folders=False):
        res = result
        assert sorted(flatten_nested_structure(res)) in valid_results


def test_iter_package_and_prepare_for_upload_with_filter(tmp_path, my_package_dir):
    valid_results = {
        "file": tmp_path / "my_data_package" / "script.py",
        "folder": {},
    }
    for result in iter_package(
        my_package_dir, ignore_folders=False, include_pattern=r"\.py"
    ):
        assert result == valid_results

    valid_results = [
        {
            "file": tmp_path / "my_data_package" / "script.py",
            "folder": {},
        },
        {
            "file": tmp_path / "my_data_package" / "readme.md",
            "folder": {},
        },
    ]
    for result in iter_package(
        my_package_dir,
        ignore_folders=False,
        exclude_pattern=r"test_folder1|test_folder2",
    ):
        assert result in valid_results

    # exclude wins!
    for result in iter_package(
        my_package_dir,
        ignore_folders=False,
        include_pattern="test_",
        exclude_pattern=r"test_folder1|test_folder2",
    ):
        assert result in valid_results

    for result in iter_package(
        my_package_dir, ignore_folders=False, exclude_pattern=r"test_folder2|\.py|\.md"
    ):
        assert result == {
            "file": "",
            "folder": {
                "location": tmp_path / "my_data_package" / "test_folder1",
                "root_folder": tmp_path / "my_data_package",
                "archive_destination": tmp_path
                / "my_data_package"
                / TEMPORARY_DIRECTORY_NAME
                / "test_folder1",
                "files": [tmp_path / "my_data_package" / "test_folder1" / "text.txt"],
            },
        }


def test_find_archive(tmp_path):
    (tmp_path / "abc.tar.gz.json").touch()
    (tmp_path / "abc.gz.json").touch()
    (tmp_path / "abc.gz").touch()

    assert tmp_path / "abc.gz" == find_archive(tmp_path / "abc")

    (tmp_path / "abc.zip").touch()
    with pytest.raises(AssertionError):
        find_archive(tmp_path / "abc")
