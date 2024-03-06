import json
import sys
import time

import pytest

from ckool import TEMPORARY_DIRECTORY_NAME, UPLOAD_IN_PROGRESS_STRING
from ckool.api import _upload_package
from ckool.other.types import CompressionTypes, HashTypes


@pytest.mark.impure
def test_upload_package_nothing_to_upload_sequential(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    config_section_instance,
):
    del config_section_instance["section"]

    (tmp_path / "f1").mkdir()
    (tmp_path / "f1" / "absas.txt").write_text("asada")

    _ = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )


@pytest.mark.impure
def test_upload_package_nothing_to_upload_parallel(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    config_section_instance,
):
    del config_section_instance["section"]

    (tmp_path / "f1").mkdir()
    (tmp_path / "f1" / "absas.txt").write_text("asada")

    _ = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=True,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )


@pytest.mark.slow
@pytest.mark.impure
def test_upload_package_sequential(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
):
    del config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=very_large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": "large_0.bin", "status": "normal"},
            {"name": "large_1.bin", "status": "normal"},
            {"name": "large_2.bin", "status": "normal"},
            {"name": "large_3.bin", "status": "normal"},
        ]


@pytest.mark.slow
@pytest.mark.impure
def test_upload_package_parallel(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
):
    del config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=very_large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=True,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": "large_0.bin", "status": "normal"},
            {"name": "large_1.bin", "status": "normal"},
            {"name": "large_2.bin", "status": "normal"},
            {"name": "large_3.bin", "status": "normal"},
        ]


@pytest.mark.slow
@pytest.mark.impure
def test_upload_package_sequential_with_compression(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
):
    del config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=very_large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": "large_0.bin", "status": "normal"},
            {"name": "large_1.bin", "status": "normal"},
            {"name": "large_2.bin", "status": "normal"},
            {"name": "large_3.bin", "status": "normal"},
        ]


@pytest.mark.slow
@pytest.mark.impure
def test_upload_package_parallel_with_compression(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
):
    del config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=very_large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=True,
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": "large_0.bin", "status": "normal"},
            {"name": "large_1.bin", "status": "normal"},
            {"name": "large_2.bin", "status": "normal"},
            {"name": "large_3.bin", "status": "normal"},
        ]


@pytest.mark.slow
@pytest.mark.impure
@pytest.mark.parametrize(
    "ckan_root_disk_size, scp_upload, status",
    [
        (1, True, "replaced"),
        (20 * 1024**3, False, "normal")
    ]
)
def test_upload_package_interrupted(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    pretty_large_file,
    config_section_instance,
    run_with_timeout,
    ckan_root_disk_size,
        scp_upload,
        status,
):

    saved_meta = {
        "file": pretty_large_file.as_posix(),
         "hash": "dabbb3155e4e21bcd895b269391aa3ac065b9fc7815e8b761f39b25c8854b92b",
         "hashtype": "sha256",
         "size": pretty_large_file.stat().st_size,
         "format": "bin"
     }
    (tmp := (tmp_path / TEMPORARY_DIRECTORY_NAME)).mkdir()
    (tmp / "pretty_large.bin.json").write_text(
        json.dumps(
            saved_meta
        )
    )

    del config_section_instance["section"]
    config_section_instance["config"]["Test"]["other"][0]["space_available_on_server_root_disk"] = ckan_root_disk_size
    len_before = len(ckan_instance.get_package(ckan_envvars["test_package"])["resources"])
    uploaded = run_with_timeout(
        _upload_package,
        timeout=1.5,
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        progressbar=False,
        **config_section_instance,
    )

    resources = ckan_instance.get_package(ckan_envvars["test_package"])["resources"]
    if scp_upload:
        assert len(resources) == len_before + 1
        assert any([r["hash"] == UPLOAD_IN_PROGRESS_STRING for r in resources])

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        progressbar=False,
        **config_section_instance,
    )

    assert uploaded[0]["status"] == status
    resources = ckan_instance.get_package(ckan_envvars["test_package"])["resources"]
    assert len(resources) == 2
    assert any([r["hash"] == saved_meta["hash"] for r in resources])

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=False,
        workers=4,
        verify=False,
        test=True,
        progressbar=False,
        **config_section_instance,
    )

    assert uploaded[0]["status"] == "skipped"

    time.sleep(2)
