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
                for file in iter_files(tmp_path, include_pattern="\.py")
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

    for result in iter_package(my_package_dir):
        res = result
        assert sorted(flatten_nested_structure(res)) in valid_results


def test_iter_package_and_prepare_for_upload_with_filter(tmp_path, my_package_dir):
    valid_results = {
        "file": tmp_path / "my_data_package" / "script.py",
        "folder": {},
    }
    for result in iter_package(my_package_dir, include_pattern=r"\.py"):
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
        my_package_dir, exclude_pattern=r"test_folder1|test_folder2"
    ):
        assert result in valid_results

    # exclude wins!
    for result in iter_package(
        my_package_dir,
        include_pattern="test_",
        exclude_pattern=r"test_folder1|test_folder2",
    ):
        assert result in valid_results

    for result in iter_package(
        my_package_dir, exclude_pattern=r"test_folder2|\.py|\.md"
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


# def test_prepare_for_upload_sequential(tmp_path, my_package_dir):
#     def filter_hash(x):
#         return {"file": x["file"], "size": x["size"]}
#
#     correct = [
#         {
#             "file": (tmp_path / "my_data_package" / "readme.md").as_posix(),
#             "size": 0,
#         },
#         {
#             "file": (tmp_path / "my_data_package" / "script.py").as_posix(),
#             "size": 0,
#         },
#         {
#             "file": (
#                 tmp_path
#                 / "my_data_package"
#                 / TEMPORARY_DIRECTORY
#                 / "test_folder2.tar.gz"
#             ).as_posix(),
#             "size": 139,
#         },
#         {
#             "file": (
#                 tmp_path
#                 / "my_data_package"
#                 / TEMPORARY_DIRECTORY
#                 / "test_folder1.tar.gz"
#             ).as_posix(),
#             "size": 122,
#         },
#     ]
#
#     files = prepare_for_upload_sequential(my_package_dir, compression_type=CompressionTypes.tar_gz)
#     for entry in files:
#         assert filter_hash(entry) in correct
#
#
# def test_prepare_for_upload_parallel(tmp_path, my_package_dir):
#     def filter_hash(x):
#         return {"file": x["file"], "size": x["size"]}
#
#     files = prepare_for_upload_parallel(my_package_dir, compression_type=CompressionTypes.tar_gz)
#     should_be = [
#         {
#             "file": (tmp_path / "my_data_package" / "readme.md").as_posix(),
#             "size": 0,
#         },
#         {
#             "file": (tmp_path / "my_data_package" / "script.py").as_posix(),
#             "size": 0,
#         },
#         {
#             "file": (
#                 tmp_path
#                 / "my_data_package"
#                 / TEMPORARY_DIRECTORY
#                 / "test_folder2.tar.gz"
#             ).as_posix(),
#             "size": 139,
#         },
#         {
#             "file": (
#                 tmp_path
#                 / "my_data_package"
#                 / TEMPORARY_DIRECTORY
#                 / "test_folder1.tar.gz"
#             ).as_posix(),
#             "size": 122,
#         },
#     ]
#     for file in files:
#         assert filter_hash(file) in should_be
#
#
# def test_prepare_for_upload_performance(tmp_path, large_package):
#     t1 = time.time()
#     files_1 = prepare_for_upload_sequential(large_package, compression_type=CompressionTypes.zip)
#     t2 = time.time()
#
#     shutil.rmtree(
#         large_package / TEMPORARY_DIRECTORY
#     )  # deletes the tmp folder from the first preparation
#
#     t3 = time.time()
#     files_2 = prepare_for_upload_parallel(large_package, compression_type=CompressionTypes.zip)
#     t4 = time.time()
#
#     duration_sequential = t2 - t1
#     duration_parallel = t4 - t3
#     assert files_1 == files_2
#     # There should be a significant difference in performance.
#     # Especially for large files, if system resources are available.
#     # assert duration_parallel < duration_sequential, (
#     #    f"Performance parallel {duration_parallel:.4f}s\n"
#     #    f"Performance sequential: {duration_sequential:.4f}s\n"
#     #    f"Parallel is not faster "
#     # )


def test_find_archive(tmp_path):
    (tmp_path / "abc.tar.gz.json").touch()
    (tmp_path / "abc.gz.json").touch()
    (tmp_path / "abc.gz").touch()

    assert tmp_path / "abc.gz" == find_archive(tmp_path / "abc")

    (tmp_path / "abc.zip").touch()
    with pytest.raises(AssertionError):
        find_archive(tmp_path / "abc")
