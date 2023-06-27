import pytest


@pytest.mark.slow_or_impure
def test_remote_interface(remote_interface, tmp_path):
    test_file = tmp_path / "test_file.txt"
    test_file.write_text("test")

    response = remote_interface.call_action("package_list")
    assert len(response) >= 1

    response = remote_interface.action.resource_create(
        package_id="test_package",
        name="pytest_resource",
        resource_type="Dataset",
        restricted_level="public",
        format="txt",
        size="1024",
        upload=(test_file.name, test_file.open('rb'))
    )
    assert response["url"].endswith(test_file.name)
