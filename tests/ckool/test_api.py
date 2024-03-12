import json
import time

import pytest

from ckool import TEMPORARY_DIRECTORY_NAME, UPLOAD_IN_PROGRESS_STRING
from ckool.api import _prepare_package, _upload_package, _download_resource
from ckool.other.caching import read_cache
from ckool.other.types import CompressionTypes, HashTypes

SWITCH = {"parallel": True, "sequential": False, "ignore": False, "overwrite": True}


@pytest.mark.impure
@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
def test_upload_package_nothing_to_upload_sequential(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    config_section_instance,
    run_type,
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
        parallel=SWITCH.get(run_type),
        workers=4,
        verify=False,
        test=True,
        **config_section_instance,
    )


@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
@pytest.mark.slow
@pytest.mark.impure
def test_upload_package(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
    run_type,
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
        parallel=SWITCH.get(run_type),
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
@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
def test_upload_package_with_compression(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    very_large_package,
    config_section_instance,
    run_type,
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
        parallel=SWITCH.get(run_type),
        workers=4,
        verify=False,
        test=True,
        progressbar=True,
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

    assert all(
        [
            r.get("hash") != UPLOAD_IN_PROGRESS_STRING
            for r in ckan_instance.get_package(ckan_envvars["test_package"])[
                "resources"
            ]
        ]
    )

    uploaded = _upload_package(
        package_name=ckan_envvars["test_package"],
        package_folder=very_large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=SWITCH.get(run_type),
        workers=4,
        verify=False,
        test=True,
        progressbar=True,
        **config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": "large_0.bin", "status": "skipped"},
            {"name": "large_1.bin", "status": "skipped"},
            {"name": "large_2.bin", "status": "skipped"},
            {"name": "large_3.bin", "status": "skipped"},
        ]


@pytest.mark.slow
@pytest.mark.impure
@pytest.mark.parametrize(
    "ckan_root_disk_size, scp_upload, status",
    [(1, True, "replaced"), (20 * 1024**3, False, "normal")],
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
        "format": "bin",
    }
    (tmp := (tmp_path / TEMPORARY_DIRECTORY_NAME)).mkdir()
    (tmp / "pretty_large.bin.json").write_text(json.dumps(saved_meta))

    del config_section_instance["section"]
    config_section_instance["config"]["Test"]["other"][0][
        "space_available_on_server_root_disk"
    ] = ckan_root_disk_size
    len_before = len(
        ckan_instance.get_package(ckan_envvars["test_package"])["resources"]
    )
    _ = run_with_timeout(
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

    time.sleep(1)


@pytest.mark.slow
@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
@pytest.mark.parametrize("prepare_type", ["ignore", "overwrite"])
@pytest.mark.parametrize("hash_type", [HashTypes.sha256, HashTypes.md5])
@pytest.mark.parametrize(
    "compression_type",
    [
        CompressionTypes.zip,
        CompressionTypes.tar_xz,
        CompressionTypes.tar_gz,
        CompressionTypes.tar_bz2,
    ],
)
def test_prepare_package(tmp_path, run_type, hash_type, compression_type, prepare_type):
    (tmp_path / "file_1.txt").write_text("hello")
    (tmp_path / "file_2.txt").write_text("hello again")
    (tmp_path / "other.sh").write_text("I'm a script")
    (tmp_path / "strange_file.txt").write_text("so strange")
    (dir_1 := tmp_path / "dir_1").mkdir()
    (dir_2 := tmp_path / "dir_2").mkdir()
    (dir_1 / "file_3.txt").write_text("dir_1 file_1")
    (dir_1 / "file_4.txt").write_text("dir_1 file_2")
    (dir_2 / "file_5.txt").write_text("dir_2 file_1")
    (dir_2 / "file_6.txt").write_text("dir_2 file_2")

    cache_files = _prepare_package(
        tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern="file_[0-9]",
        exclude_pattern=None,
        compression_type=CompressionTypes.tar_gz,
        hash_algorithm=HashTypes.md5,
        parallel=SWITCH.get(run_type),
        ignore_prepared=SWITCH.get(prepare_type),
        progressbar=False,
    )
    assert len(cache_files) == 2
    [read_cache(f) for f in cache_files]

    cache_files = _prepare_package(
        tmp_path.as_posix(),
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern="file*",
        compression_type=CompressionTypes.tar_gz,
        hash_algorithm=HashTypes.md5,
        parallel=SWITCH.get(run_type),
        ignore_prepared=SWITCH.get(prepare_type),
        progressbar=False,
    )
    assert len(cache_files) == 1
    [read_cache(f) for f in cache_files]

    cache_files = _prepare_package(
        tmp_path.as_posix(),
        include_sub_folders=True,
        include_pattern=None,
        exclude_pattern="file_[5-6]",
        compression_type=CompressionTypes.tar_gz,
        hash_algorithm=HashTypes.md5,
        parallel=SWITCH.get(run_type),
        ignore_prepared=SWITCH.get(prepare_type),
        progressbar=False,
    )
    assert len(cache_files) == 5
    [read_cache(f) for f in cache_files]


@pytest.mark.impure
def test_download_resource(
    tmp_path,
    ckan_instance,
    ckan_envvars,
    ckan_setup_data,
    small_file,
    config_section_instance
):
    meta = {
        "file": small_file,
        "package_id": ckan_envvars["test_package"],
        "size": small_file.stat().st_size,
        "hash": "hasashasasdsadasdsadsadas",
        "format": small_file.suffix[1:],
        "hashtype": "md5",
    }
    ckan_instance.create_resource_of_type_file(**meta)
    del config_section_instance["section"]
    (dest := tmp_path / "downloads").mkdir()
    _download_resource(
        package_name=ckan_envvars["test_package"],
        resource_name=small_file.name,
        destination=dest.as_posix(),
        verify=False,
        test=True,
        **config_section_instance
    )
    assert (dest / small_file.name).exists()

