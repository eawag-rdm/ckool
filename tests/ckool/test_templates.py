import pytest

from ckool import HASH_TYPE
from ckool.other.hashing import get_hash_func
from ckool.templates import (
    get_upload_func,
    hash_remote,
    resource_integrity_between_ckan_instances_intact,
    resource_integrity_remote_intact,
    upload_resource_file_via_api,
    upload_resource_file_via_scp,
)

hasher = get_hash_func(HASH_TYPE)


@pytest.mark.impure
def test_resource_integrity_between_ckan_instances_intact(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "file": f,
        "package_id": ckan_envvars["test_package"],
        "file_size": f.stat().st_size,
        "file_hash": hasher(f),
        "file_format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }
    ckan_instance.create_resource_of_type_file(**meta)

    assert resource_integrity_between_ckan_instances_intact(
        {
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        {
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        ckan_envvars["test_package"],
        f.name,
    )


@pytest.mark.impure
def test_upload_resource_file_via_api(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }

    upload_resource_file_via_api(
        ckan_api_input={
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        filepath=f,
        package_name=ckan_envvars["test_package"],
        metadata=meta,
        progressbar=False,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])


@pytest.mark.impure
def test_upload_resource_file_via_scp(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }

    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    abc = upload_resource_file_via_scp(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_name=f.name,
    )


@pytest.mark.impure
def test_upload_func_chosen_api(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }

    upload = get_upload_func(
        file_sizes=[50 * 1024**2, 100 * 1024**2],
        space_available_on_server_root_disk=1000 * 1024**2,
        parallel_upload=False,
        factor=2,
    )
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_name=f.name,
    )


@pytest.mark.impure
def test_upload_func_chosen_scp(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }

    upload = get_upload_func(
        file_sizes=[50 * 1024**2, 100 * 1024**2],
        space_available_on_server_root_disk=100 * 1024**2,
        parallel_upload=False,
        factor=2,
    )
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_name=f.name,
    )


@pytest.mark.impure
def test_hash_remote(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "file": f,
        "package_id": ckan_envvars["test_package"],
        "file_size": f.stat().st_size,
        "file_hash": hasher(f),
        "file_format": f.suffix[1:],
        "hash_type": HASH_TYPE,
    }
    response = ckan_instance.create_resource_of_type_file(**meta)
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    hashed_remotely = hash_remote(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_name=f.name,
    )
    hashed_locally = ckan_instance.get_resource_meta(
        package_name=ckan_envvars["test_package"],
        resource_name=f.name,
    )["hash"]

    assert hashed_locally == hashed_remotely