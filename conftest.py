import json
import os
import pathlib
import random
import string
import time

import pytest

from ckool.ckan.ckan import CKAN
from ckool.datacite.datacite import DataCiteAPI
from tests.ckool.data.inputs.ckan_entity_data import *


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
    }


@pytest.fixture
def ckan_instance(ckan_envvars):
    return CKAN(
        server=ckan_envvars["host"],
        token=ckan_envvars["token"],
        verify_certificate=False,
    )


def setup(ckan_instance, ckan_envvars):
    organization_data.update({"name": f"{ckan_envvars['test_organization']}"})
    package_data.update({"name": f"{ckan_envvars['test_package']}"})
    resource_data.update({"package_id": f"{ckan_envvars['test_package']}"})

    ckan_instance.create_organization(**organization_data)
    ckan_instance.create_package(**package_data)
    ckan_instance.create_resource(**resource_data)


def teardown(ckan_instance, ckan_envvars):
    ckan_instance.delete_package(ckan_envvars["test_package"])
    ckan_instance.delete_organization(ckan_envvars["test_organization"])
    ckan_instance.purge_organization(ckan_envvars["test_organization"])


@pytest.fixture
def ckan_setup_data(ckan_instance, ckan_envvars):
    setup(ckan_instance, ckan_envvars)
    yield
    teardown(ckan_instance, ckan_envvars)


@pytest.fixture
def ckan_test_package(load_env_file):
    return os.environ["CKAN_TEST_PACKAGE_NAME"]


@pytest.fixture
def datacite_instance(load_env_file):
    return DataCiteAPI(
        username=os.environ["TEST_DATACITE_USER"],
        password=os.environ["TEST_DATACITE_PASSWORD"],
        prefix=os.environ["TEST_DATACITE_PREFIX"],
        host=os.environ["TEST_DATACITE_URL"],
        offset=os.environ["TEST_DATACITE_OFFSET"],
    )


def pytest_addoption(parser):
    parser.addoption(
        "--runall", action="store_true", default=False, help="run all tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow_or_impure: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runall"):
        # --runall given in cli: do run all tests, even slow or impure
        return
    skip_slow_or_impure = pytest.mark.skip(reason="need --runall option to run")
    for item in items:
        if "slow_or_impure" in item.keywords:
            item.add_marker(skip_slow_or_impure)


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
    """10GB"""
    yield from generate_binary_file(10 * 1024**3, tmp_path, "very_large.bin")


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


@pytest.fixture()
def large_package(tmp_path, my_package_dir):
    file_sizes = 1024**2 * 15
    file_name = "large.txt"

    files = []
    for folder in [my_package_dir / f"folder_{i}" for i in range(10)]:
        folder.mkdir()
        files.append(_generate_binary_file(file_sizes, folder, file_name))

    yield my_package_dir

    for file in files:
        file.unlink()


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
