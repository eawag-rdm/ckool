import pytest

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
