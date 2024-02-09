import pytest

from ckool.full_upload_flow import FullUploadFlow


def test_full_upload_flow_init(my_package_dir):
    fuf = FullUploadFlow(
        package_name=my_package_dir.name,
        package_folder=my_package_dir,
    )
    assert fuf.total_job_estimate == {"multi-threaded": 4, "multi-processed": 6}


@pytest.mark.impure
def test_full_upload_flow_run_flow(
    my_package_dir, ckan_instance, ckan_envvars, ckan_setup_data
):
    fuf = FullUploadFlow(
        package_name=my_package_dir.name,
        package_folder=my_package_dir,
        ckan_api_input={
            "server": ckan_instance.server,
            "token": ckan_instance.token,
            "verify_certificate": ckan_instance.verify,
        },
    )

    fuf.run_flow()


@pytest.mark.impure
@pytest.mark.slow
def test_full_upload_flow_run_flow_large_files(
    very_large_package, ckan_instance, ckan_envvars, ckan_setup_data
):
    fuf = FullUploadFlow(
        package_name=very_large_package.name,
        package_folder=very_large_package,
        ckan_api_input={
            "server": ckan_instance.server,
            "token": ckan_instance.token,
            "verify_certificate": ckan_instance.verify,
        },
    )

    fuf.run_flow()
