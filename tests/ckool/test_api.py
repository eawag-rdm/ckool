import json
import time
from unittest.mock import Mock

import pytest
from conftest import ckan_instance_names_of_fixtures

from ckool import TEMPORARY_DIRECTORY_NAME, UPLOAD_IN_PROGRESS_STRING
from ckool.api import (
    _download_resource,
    _prepare_package,
    _publish_package,
    _upload_package,
    _upload_resource,
)
from ckool.other.caching import read_cache
from ckool.other.types import CompressionTypes, HashTypes

SWITCH = {"parallel": True, "sequential": False, "ignore": False, "overwrite": True}


@pytest.mark.impure
@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
def test_upload_package_nothing_to_upload_sequential(
    cki,
    tmp_path,
    ckan_entities,
    dynamic_ckan_setup_data,
    dynamic_config_section_instance,
    run_type,
):
    del dynamic_config_section_instance["section"]
    (tmp_path / "f1").mkdir()
    (tmp_path / "f1" / "absas.txt").write_text("asada")

    _ = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
    )


@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.slow
@pytest.mark.impure
def test_upload_package(
    cki,
    tmp_path,
    ckan_entities,
    dynamic_ckan_instance,
    dynamic_ckan_setup_data,
    very_large_package,
    dynamic_config_section_instance,
    run_type,
):
    del dynamic_config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": f"large_{i}.bin", "status": "normal"} for i in range(4)
        ]


@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.slow
@pytest.mark.impure
def test_upload_package_separate_uploads(
    cki,
    tmp_path,
    ckan_entities,
    dynamic_ckan_instance,
    dynamic_ckan_setup_data,
    large_package,
    very_large_package,
    dynamic_config_section_instance,
    run_type,
):
    del dynamic_config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
    )

    for entry in uploaded:
        del entry["id"]
        assert entry in [
            {"name": f"large_{i}.bin", "status": "normal"} for i in range(4)
        ]

    uploaded_also = _upload_package(
        package_name=ckan_entities["test_package"],
        package_folder=large_package,
        include_sub_folders=False,
        include_pattern=None,
        exclude_pattern=None,
        hash_algorithm=HashTypes.sha256,
        compression_type=CompressionTypes.zip,
        parallel=SWITCH.get(run_type),
        workers=4,
        verify=False,
        test=True,
        **dynamic_config_section_instance,
    )

    for entry in uploaded_also:
        del entry["id"]
        assert entry in [
            {"name": f"large_{i}.bin", "status": "replaced"} for i in range(4)
        ] + [{"name": f"large_{i}.bin", "status": "normal"} for i in range(4, 10)]


@pytest.mark.slow
@pytest.mark.impure
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.parametrize("run_type", ["parallel", "sequential"])
def test_upload_package_with_compression(
    cki,
    tmp_path,
    ckan_entities,
    dynamic_ckan_instance,
    dynamic_ckan_setup_data,
    very_large_package,
    dynamic_config_section_instance,
    run_type,
):
    del dynamic_config_section_instance["section"]

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
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
            for r in dynamic_ckan_instance.get_package(ckan_entities["test_package"])[
                "resources"
            ]
        ]
    )

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
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
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.parametrize(
    "ckan_root_disk_size, scp_upload, status",
    [(1, True, "replaced"), (20 * 1024**3, False, "normal")],
)
def test_upload_package_interrupted(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    ckan_entities,
    dynamic_ckan_setup_data,
    pretty_large_file,
    dynamic_config_section_instance,
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

    del dynamic_config_section_instance["section"]
    dynamic_config_section_instance["config"]["Test"]["other"][0][
        "space_available_on_server_root_disk"
    ] = ckan_root_disk_size
    len_before = len(
        dynamic_ckan_instance.get_package(ckan_entities["test_package"])["resources"]
    )
    _ = run_with_timeout(
        _upload_package,
        timeout=1.5,
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
    )

    resources = dynamic_ckan_instance.get_package(ckan_entities["test_package"])[
        "resources"
    ]
    if scp_upload:
        assert len(resources) == len_before + 1
        assert any([r["hash"] == UPLOAD_IN_PROGRESS_STRING for r in resources])

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
    )
    assert uploaded[0]["status"] == status
    resources = dynamic_ckan_instance.get_package(ckan_entities["test_package"])[
        "resources"
    ]
    assert len(resources) == 2
    assert any([r["hash"] == saved_meta["hash"] for r in resources])

    uploaded = _upload_package(
        package_name=ckan_entities["test_package"],
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
        **dynamic_config_section_instance,
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
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
def test_download_resource(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    ckan_entities,
    dynamic_ckan_setup_data,
    small_file,
    dynamic_config_section_instance,
):
    meta = {
        "file": small_file,
        "package_id": ckan_entities["test_package"],
        "size": small_file.stat().st_size,
        "hash": "hasashasasdsadasdsadsadas",
        "format": small_file.suffix[1:],
        "hashtype": "md5",
    }
    dynamic_ckan_instance.create_resource_of_type_file(**meta)
    del dynamic_config_section_instance["section"]
    (dest := tmp_path / "downloads").mkdir()
    _download_resource(
        package_name=ckan_entities["test_package"],
        resource_name=small_file.name,
        destination=dest.as_posix(),
        verify=False,
        test=True,
        **dynamic_config_section_instance,
    )
    assert (dest / small_file.name).exists()


@pytest.mark.impure
@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
def test_upload_resource(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    ckan_entities,
    dynamic_ckan_setup_data,
    small_file,
    dynamic_config_section_instance,
):
    del dynamic_config_section_instance["section"]

    _upload_resource(
        package_name=ckan_entities["test_package"],
        filepath=small_file,
        hash_algorithm=HashTypes.md5,
        verify=False,
        test=True,
        **dynamic_config_section_instance,
    )
    meta = dynamic_ckan_instance.get_resource_meta(
        package_name=ckan_entities["test_package"], resource_id_or_name=small_file.name
    )
    assert meta
    assert (
        meta["hash"]
        == read_cache(
            tmp_path / TEMPORARY_DIRECTORY_NAME / (small_file.name + ".json")
        )["hash"]
    )


@pytest.mark.slow
@pytest.mark.open
@pytest.mark.impure
@pytest.mark.parametrize(
    "projects_to_publish", ("test_group", None)
)  # This value can be specified in the .env file!
@pytest.mark.parametrize("check_data_integrity", (True, False))
@pytest.mark.parametrize(
    "exclude_resources", ("file_0,file_1", None)
)  # resource names a re specified in conftest.py
@pytest.mark.parametrize("only_hash_source_if_missing", (True, False))
@pytest.mark.parametrize("re_download_resources", (True, False))
def test_publish_package_simple(
    tmp_path,
    doi_setup,
    ckan_instance_name_internal,
    ckan_instance_name_open,
    full_config,
    ckan_setup_data,
    ckan_instance,
    ckan_open_instance,
    ckan_open_cleanup,
    ckan_entities,
    add_file_resources,
    projects_to_publish,
    check_data_integrity,
    exclude_resources,
    only_hash_source_if_missing,
    re_download_resources,
):
    mock_prompt = Mock()  # mocking prompt function
    mock_prompt.side_effect = [
        "no",  # no to providing orcids
        "no",  # no to providing affiliations
        "no",  # no to providing related_identifiers
        "no",  # no to publishing the doi
    ] * 2  # running publishing twice

    add_file_resources(
        ckan_instance,
        [
            4 * 1024**2,  # file_0
            5 * 1024**2,  # file_1
            6 * 1024**2,  # file_2
            7 * 1024**2,  # file_3
        ],
    )

    for resource in ckan_instance.get_package(
        package_name=ckan_entities["test_package"]
    )["resources"]:
        _id = resource["id"]
        ckan_instance.patch_resource_metadata(
            resource_id=_id, resource_data_to_update={"hash": "", "hashtype": "md5"}
        )

    ckan_instance.patch_package_metadata(
        package_id=ckan_entities["test_package"],
        data={
            "geographic_name": ["Switzerland"]
        },  # this field is required for the publication (required on ERIC Open)
    )

    ckan_instance.add_package_to_project(
        package_name=ckan_entities["test_package"],
        project_name=ckan_entities["test_project"],
    )
    _publish_package(
        package_name=ckan_entities["test_package"],
        projects_to_publish=projects_to_publish,
        check_data_integrity=check_data_integrity,
        create_missing_=True,
        exclude_resources=exclude_resources,
        only_hash_source_if_missing=only_hash_source_if_missing,
        re_download_resources=re_download_resources,
        no_resource_overwrite_prompt=True,
        ckan_instance_source=ckan_instance_name_internal,
        config=full_config,
        ckan_instance_target=ckan_instance_name_open,
        verify=False,
        test=True,
        prompt_function=mock_prompt,
        working_directory=tmp_path.as_posix(),
    )
    number_of_resources = len(
        ckan_open_instance.get_package(package_name=ckan_entities["test_package"])[
            "resources"
        ]
    )

    if isinstance(exclude_resources, str):
        assert number_of_resources == 3  # 2 files, 1 link
    else:
        assert number_of_resources == 5  # 4 files, 1 link

    number_of_projects = len(ckan_open_instance.get_all_projects())
    if isinstance(projects_to_publish, str):
        assert number_of_projects == 1
    else:
        assert number_of_projects == 0

    # Delete some stuff on eric_open
    resource_id = ckan_open_instance.resolve_resource_id_or_name_to_id(
        package_name=ckan_entities["test_package"], resource_id_or_name="file_2"
    )["id"]
    ckan_open_instance.delete_resource(resource_id=resource_id)

    # Running a second time to simulate republishing
    _publish_package(
        package_name=ckan_entities["test_package"],
        projects_to_publish=projects_to_publish,
        check_data_integrity=check_data_integrity,
        create_missing_=True,  # nothing missing at this point
        exclude_resources=exclude_resources,
        only_hash_source_if_missing=only_hash_source_if_missing,
        re_download_resources=re_download_resources,
        no_resource_overwrite_prompt=True,
        ckan_instance_source=ckan_instance_name_internal,
        config=full_config,
        ckan_instance_target=ckan_instance_name_open,
        verify=False,
        test=True,
        prompt_function=mock_prompt,
        working_directory=tmp_path.as_posix(),
    )

    number_of_resources = len(
        ckan_open_instance.get_package(package_name=ckan_entities["test_package"])[
            "resources"
        ]
    )
    if isinstance(exclude_resources, str):
        assert number_of_resources == 3  # 2 files, 1 link
    else:
        assert number_of_resources == 5  # 4 files, 1 link


@pytest.mark.slow
@pytest.mark.open
@pytest.mark.impure
@pytest.mark.parametrize(
    "projects_to_publish", ("test_group", None)
)  # This value can be specified in the .env file!
@pytest.mark.parametrize("check_data_integrity", (True, False))
@pytest.mark.parametrize(
    "exclude_resources", ("file_0", None)
)  # resource names a re specified in conftest.py
@pytest.mark.parametrize("only_hash_source_if_missing", (True, False))
@pytest.mark.parametrize("re_download_resources", (True, False))
def test_publish_package_do_not_create_missing(
    tmp_path,
    doi_setup,
    ckan_instance_name_internal,
    ckan_instance_name_open,
    full_config,
    ckan_setup_data,
    ckan_instance,
    ckan_open_instance,
    ckan_open_cleanup,
    ckan_entities,
    add_file_resources,
    projects_to_publish,
    check_data_integrity,
    exclude_resources,
    only_hash_source_if_missing,
    re_download_resources,
):
    mock_prompt = Mock()
    mock_prompt.side_effect = [
        "no",  # no to providing orcids
        "no",  # no to providing affiliations
        "no",  # no to providing related_identifiers
        "no",  # no to publishing the doi
    ] * 2

    add_file_resources(
        ckan_instance,
        [
            4 * 1024**2,  # file_0
            5 * 1024**2,  # file_1
        ],
    )
    for resource in ckan_instance.get_package(
        package_name=ckan_entities["test_package"]
    )["resources"]:
        _id = resource["id"]
        ckan_instance.patch_resource_metadata(
            resource_id=_id, resource_data_to_update={"hash": "", "hashtype": "md5"}
        )

    ckan_instance.patch_package_metadata(
        package_id=ckan_entities["test_package"],
        data={
            "geographic_name": ["Switzerland"]
        },  # this field is required for the publication (required on ERIC Open)
    )

    ckan_instance.add_package_to_project(
        package_name=ckan_entities["test_package"],
        project_name=ckan_entities["test_project"],
    )
    with pytest.raises(ValueError) as exc_info:
        _publish_package(
            package_name=ckan_entities["test_package"],
            projects_to_publish=projects_to_publish,
            check_data_integrity=check_data_integrity,
            create_missing_=False,
            exclude_resources=exclude_resources,
            only_hash_source_if_missing=only_hash_source_if_missing,
            re_download_resources=re_download_resources,
            no_resource_overwrite_prompt=True,
            ckan_instance_source=ckan_instance_name_internal,
            config=full_config,
            ckan_instance_target=ckan_instance_name_open,
            verify=False,
            test=True,
            prompt_function=mock_prompt,
            working_directory=tmp_path.as_posix(),
        )
        assert "Publication can not continue. These entities are missing:" in str(
            exc_info.value
        )
