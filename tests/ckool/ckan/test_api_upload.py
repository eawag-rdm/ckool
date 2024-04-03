import pytest
from conftest import ckan_instance_names_of_fixtures

from ckool import HASH_TYPE
from ckool.ckan.upload import upload_resource
from ckool.other.hashing import get_hash_func

hasher = get_hash_func(HASH_TYPE)


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_small(
    cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities, small_file
):
    response = upload_resource(
        small_file,
        ckan_entities["test_package"],
        dynamic_ckan_instance.server,
        dynamic_ckan_instance.token,
        hash=hasher(small_file),
        size=small_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_small_all_args(
    cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities, small_file
):
    response = upload_resource(
        small_file,
        ckan_entities["test_package"],
        dynamic_ckan_instance.server,
        dynamic_ckan_instance.token,
        hash=hasher(small_file),
        size=small_file.stat().st_size,
        citation="text",
        description="text",
        format=".bin",
        hashtype=HASH_TYPE,
        resource_type="Dataset",
        restricted_level="public",
        state="active",
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_large(
    cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities, large_file
):
    response = upload_resource(
        large_file,
        ckan_entities["test_package"],
        dynamic_ckan_instance.server,
        dynamic_ckan_instance.token,
        hash=hasher(large_file),
        size=large_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.slow
@pytest.mark.impure
def test_upload_very_large(
    cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities, very_large_file
):
    response = upload_resource(
        very_large_file,
        ckan_entities["test_package"],
        dynamic_ckan_instance.server,
        dynamic_ckan_instance.token,
        hash=hasher(very_large_file),
        size=very_large_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()
