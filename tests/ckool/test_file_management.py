import pytest
from pytest_unordered import unordered

from ckool.file_management import (
    generate_archive_dest,
    glob_files,
    prepare_for_package_upload,
    tar_files,
    zip_files,
)


def test_iter_files(tmp_path, my_package_dir):
    assert len([file.relative_to(tmp_path) for file in glob_files(tmp_path)]) == 4
    assert (
        len(
            [
                file.relative_to(tmp_path)
                for file in glob_files(tmp_path, pattern="**/*.txt")
            ]
        )
        == 1
    )
    assert (
        len(
            [
                file.relative_to(tmp_path)
                for file in glob_files(tmp_path, pattern="**/script*")
            ]
        )
        == 1
    )


def test_generate_tmp_file_paths(tmp_path, my_package_dir):
    assert generate_archive_dest(
        my_package_dir, tmp_path, tmp_dir_name="__tmp_eric__"
    ) == (my_package_dir.parent / "__tmp_eric__" / my_package_dir.name)


def test_zip_files(tmp_path, my_package_dir):
    archive_file = generate_archive_dest(
        my_package_dir, tmp_path, tmp_dir_name="__tmp_eric__"
    )

    file = zip_files(
        my_package_dir, archive_file, [file for file in glob_files(tmp_path)]
    )

    assert file == archive_file.with_suffix(".zip")


def test_tar_files(tmp_path, my_package_dir):
    archive_file = generate_archive_dest(
        my_package_dir, tmp_path, tmp_dir_name="__tmp_eric__"
    )

    file = tar_files(
        my_package_dir, archive_file, [file for file in glob_files(tmp_path)]
    )

    assert file == archive_file.with_suffix(".tar")


def test_prepare_for_package_upload_main_folder(tmp_path, my_package_dir):
    tmp_dir_name = "__tmp__"
    assert prepare_for_package_upload(my_package_dir, tmp_dir_name=tmp_dir_name) == {
        "files": [tmp_path / tmp_dir_name / my_package_dir.with_suffix(".zip").name],
        "created": [tmp_path / tmp_dir_name / my_package_dir.with_suffix(".zip").name],
    }


def test_prepare_for_package_upload_sub_folders(tmp_path, my_package_dir):
    tmp_dir_name = "__tmp__"
    assert prepare_for_package_upload(
        my_package_dir / "*", tmp_dir_name=tmp_dir_name
    ) == {
        "files": unordered(
            [
                tmp_path / my_package_dir / tmp_dir_name / "test_folder1.zip",
                tmp_path / my_package_dir / tmp_dir_name / "test_folder2.zip",
                tmp_path / my_package_dir / "script.py",
            ]
        ),
        "created": unordered(
            [
                tmp_path / my_package_dir / tmp_dir_name / "test_folder1.zip",
                tmp_path / my_package_dir / tmp_dir_name / "test_folder2.zip",
            ]
        ),
    }


def test_prepare_for_package_upload_single_file(tmp_path, my_package_dir):
    assert prepare_for_package_upload(my_package_dir / "script.py") == {
        "files": [my_package_dir / "script.py"],
        "created": [],
    }


def test_prepare_for_package_upload_empty_folder(tmp_path, my_package_dir):
    assert prepare_for_package_upload(
        my_package_dir / "test_folder3" / "test_folder3"
    ) == {"files": [], "created": []}


def test_prepare_for_package_upload_invalid(tmp_path, my_package_dir):
    with pytest.raises(NotADirectoryError):
        prepare_for_package_upload(my_package_dir / "does_not_exist")
        prepare_for_package_upload(my_package_dir / "does_not_exist" / "*")


def test_prepare_for_package_upload_no_compression_needed(tmp_path, my_package_dir):
    assert prepare_for_package_upload(my_package_dir / "test_folder1" / "*") == {
        "files": [my_package_dir / "test_folder1" / "text.txt"],
        "created": [],
    }
