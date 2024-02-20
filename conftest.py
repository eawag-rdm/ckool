import json
import os
import pathlib

import ckanapi
import pytest

from ckool.ckan.ckan import CKAN
from ckool.ckan.upload import upload_resource
from ckool.datacite.datacite import DataCiteAPI
from tests.ckool.data.inputs.ckan_entity_data import *


def flatten_nested_structure(structure: dict | list):
    result = []

    def flatten_element(element, prefix=""):
        if isinstance(element, dict):
            for key, value in element.items():
                flatten_element(value, prefix + str(key))
        elif isinstance(element, list):
            for item in element:
                flatten_element(item, prefix)
        else:
            result.append(prefix + str(element))

    flatten_element(structure)
    return result


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-impure", action="store_true", default=False, help="run impure tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks test as slow")
    config.addinivalue_line(
        "markers", "impure: marks test as impure (depending on other systems)"
    )


def pytest_collection_modifyitems(config, items):
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    skip_impure = pytest.mark.skip(reason="need --run-impure option to run")

    if config.getoption("--run-slow") and config.getoption("--run-impure"):
        return

    for item in items:
        if "slow" in item.keywords and not config.getoption("--run-slow"):
            item.add_marker(skip_slow)
        elif "impure" in item.keywords and not config.getoption("--run-impure"):
            item.add_marker(skip_impure)


@pytest.fixture
def data_directory():
    return (
        pathlib.Path(__file__).parent.resolve() / "tests" / "ckool" / "data" / "inputs"
    )


@pytest.fixture
def valid_outputs():
    return (
        pathlib.Path(__file__).parent.resolve() / "tests" / "ckool" / "data" / "outputs"
    )


@pytest.fixture
def json_test_data(data_directory):
    data = {}
    for file in data_directory.iterdir():
        if file.is_file() and file.suffix == ".json":
            with file.open() as f:
                data[file.with_suffix("").name] = json.load(f)
    return data


@pytest.fixture
def load_env_file():
    with (pathlib.Path(__file__).parent.resolve() / ".env").open() as env:
        for line in env:
            if line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.rstrip()] = value.lstrip().rstrip("\n")


@pytest.fixture
def secure_interface_input_args(load_env_file):
    return {
        "host": os.environ.get("SECURE_INTERFACE_HOST"),
        "port": os.environ.get("SECURE_INTERFACE_PORT"),
        "username": os.environ.get("SECURE_INTERFACE_USERNAME"),
        "ssh_key": os.environ.get("SECURE_INTERFACE_SSH_KEY"),
    }


@pytest.fixture()
def ckan_envvars(load_env_file):
    return {
        "host": os.environ.get("CKAN_URL"),
        "token": os.environ.get("CKAN_TOKEN"),
        "test_package": os.environ.get("CKAN_TEST_PACKAGE_NAME"),
        "test_organization": os.environ.get("CKAN_TEST_ORGANIZATION_NAME"),
        "test_project": os.environ.get("CKAN_TEST_GROUP_NAME"),
        "test_resource": os.environ.get("CKAN_TEST_RESOURCE_NAME"),
        "storage_path": os.environ.get("CKAN_STORAGE_PATH"),
    }


@pytest.fixture
def ckan_instance(ckan_envvars):
    return CKAN(
        server=ckan_envvars["host"],
        token=ckan_envvars["token"],
        verify_certificate=False,
    )


def patch_user(ckan_instance):
    user_record = ckan_instance.get_user("ckan_admin")
    if not user_record["fullname"]:
        ckan_instance.patch_user(user_id=user_record["id"], data={"fullname": "ckan_admin"})


def setup(ckan_instance, ckan_envvars):
    patch_user(ckan_instance)

    organization_data.update({"name": ckan_envvars["test_organization"]})
    package_data.update({"name": ckan_envvars["test_package"]})
    resource_data.update(
        {
            "package_id": ckan_envvars["test_package"],
            "name": ckan_envvars["test_resource"],
        }
    )
    project_data.update({"name": ckan_envvars["test_project"]})
    try:
        ckan_instance.create_organization(**organization_data)
    except ckanapi.ValidationError:
        pass
    try:
        ckan_instance.create_package(**package_data)
    except ckanapi.ValidationError:
        pass
    try:
        ckan_instance.create_resource_of_type_link(**resource_data)
    except ckanapi.ValidationError:
        pass
    try:
        ckan_instance.create_project(**project_data)
    except ckanapi.ValidationError:
        pass


def teardown(ckan_instance, ckan_envvars):
    packages = ckan_instance.get_all_packages()["results"]
    for package in packages:
        ckan_instance.delete_package(package["id"])
    ckan_instance.delete_project(ckan_envvars["test_project"])
    ckan_instance.purge_project(ckan_envvars["test_project"])
    ckan_instance.delete_organization(ckan_envvars["test_organization"])
    ckan_instance.purge_organization(ckan_envvars["test_organization"])


@pytest.fixture()
def add_file_resources(tmp_path, ckan_instance, ckan_envvars):
    def _add_file_resources(package_sizes: list):
        files = []
        for i in range(len(package_sizes)):
            with open(
                file := _generate_binary_file(
                    package_sizes[i], tmp_path, f"file_{i}", chunk_size=4 * 1024**2
                ),
                "rb",
            ) as _file:
                upload_resource(
                    file_path=file,
                    package_id=ckan_envvars["test_package"],
                    ckan_url=ckan_envvars["host"],
                    api_key=ckan_envvars["token"],
                    size=file.stat().st_size,
                    hash="absd",
                    resource_type="Dataset",
                    restricted_level="public",
                    verify=False,
                )
            files.append(file)

        for f in files:
            f.unlink()

    return _add_file_resources


@pytest.fixture
def ckan_setup_data(ckan_instance, ckan_envvars):
    setup(ckan_instance, ckan_envvars)
    yield
    teardown(ckan_instance, ckan_envvars)


@pytest.fixture
def datacite_instance(load_env_file):
    return DataCiteAPI(
        username=os.environ["TEST_DATACITE_USER"],
        password=os.environ["TEST_DATACITE_PASSWORD"],
        prefix=os.environ["TEST_DATACITE_PREFIX"],
        host=os.environ["TEST_DATACITE_URL"],
        offset=os.environ["TEST_DATACITE_OFFSET"],
    )


def _generate_binary_file(
    size: int, path: pathlib.Path, name: str, chunk_size: int = 1024**3
):
    file_path = path / name
    chunks = []
    while size > chunk_size:
        chunks.append(chunk_size)
        size -= chunk_size
    chunks.append(size)
    with file_path.open("wb") as f:
        for chunk in chunks:
            f.write(os.urandom(chunk))
    return file_path


def generate_binary_file(
    size: int, path: pathlib.Path, name: str, chunk_size: int = 1024**3
):
    file_path = path / name
    yield _generate_binary_file(size, path, name, chunk_size)
    file_path.unlink()


@pytest.fixture
def small_file(tmp_path):
    """100KB"""
    yield from generate_binary_file(100 * 1024, tmp_path, "small.bin")


@pytest.fixture
def large_file(tmp_path):
    """100MB"""
    yield from generate_binary_file(100 * 1024**2, tmp_path, "large.bin")


@pytest.fixture
def very_large_file(tmp_path):
    """4GB"""
    yield from generate_binary_file(4 * 1024**3, tmp_path, "very_large.bin")


@pytest.fixture
def my_package_dir(tmp_path):
    (tmp_path / "my_data_package").mkdir()
    (tmp_path / "my_data_package" / "test_folder1").mkdir()
    (tmp_path / "my_data_package" / "test_folder2").mkdir()
    (tmp_path / "my_data_package" / "test_folder_empty").mkdir()
    (
        tmp_path
        / "my_data_package"
        / "test_folder_empty_nested"
        / "empty_nested"
        / "empty_nested"
    ).mkdir(parents=True)
    (tmp_path / "my_data_package" / "test_folder1" / "text.txt").touch()
    (tmp_path / "my_data_package" / "test_folder2" / "random").touch()
    (tmp_path / "my_data_package" / "test_folder2" / ".hidden").touch()
    (tmp_path / "my_data_package" / "script.py").touch()
    (tmp_path / "my_data_package" / "readme.md").touch()
    return tmp_path / "my_data_package"


def _large_package(root_folder, file_sizes, files_per_folders):
    files = []
    for u, folder in enumerate(
        [root_folder / f"folder_{i}" for i in range(len(file_sizes))]
    ):
        folder.mkdir()
        for v in range(files_per_folders):
            files.append(_generate_binary_file(file_sizes[u], folder, f"large_{v}.bin"))

    yield root_folder

    for file in files:
        file.unlink()


@pytest.fixture()
def large_package(my_package_dir):
    yield from _large_package(my_package_dir, [1024**2 * 15] * 10, 1)


@pytest.fixture()
def very_large_package(tmp_path):
    (pkg := tmp_path / "package-dir").mkdir()
    MB = 1024**2
    yield from _large_package(pkg, [10 * MB, 20 * MB, 80 * MB, 100 * MB], 8)


@pytest.fixture
def data_to_cache():
    return {
        "file": "test_file.bin",
        "hash_type": "sha256",
        "size": "1024",
        "hash": "thisismyhash",
    }


@pytest.fixture
def cache_file(tmp_path):
    return tmp_path / "cache.json"


@pytest.fixture()
def local_structure_doi(tmp_path):
    (tmp_path / "strange-file").touch()
    folders = [
        tmp_path / "name-1" / "package-url-1",
        tmp_path / "name-1" / "package-url-2",
        tmp_path / "name-2" / "package-url-2",
        tmp_path / ".git",
    ]

    files = ["file-1.txt", "file-2.pdf"]

    for fo in folders:
        fo.mkdir(parents=True)
        for fi in files:
            (fo / fi).touch()
