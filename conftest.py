import json
import os
import pathlib

import pytest

from ckool.ckan.ckan import CKAN
from ckool.datacite.datacite import DataCiteAPI


@pytest.fixture
def data_directory():
    return pathlib.Path(__file__).parent.resolve() / "tests" / "ckool" / "data"


@pytest.fixture
def valid_outputs():
    return pathlib.Path(__file__).parent.resolve() / "tests" / "ckool" / "valid_outputs"


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


@pytest.fixture
def ckan_instance(load_env_file):
    return CKAN(
        server=os.environ["CKAN_URL"],
        apikey=os.environ["CKAN_APIKEY"],
        verify_certificate=False,
    )


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


def generate_file(
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
    yield file_path
    file_path.unlink()


@pytest.fixture
def small_file(tmp_path):
    """100KB"""
    yield from generate_file(100 * 1024, tmp_path, "small.bin")


@pytest.fixture
def large_file(tmp_path):
    """100MB"""
    yield from generate_file(100 * 1024**2, tmp_path, "large.bin")


@pytest.fixture
def very_large_file(tmp_path):
    """10GB"""
    yield from generate_file(10 * 1024**3, tmp_path, "very_large.bin")


@pytest.fixture
def my_package_dir(tmp_path):
    (tmp_path / "my_data_package").mkdir()
    (tmp_path / "my_data_package" / "test_folder1").mkdir()
    (tmp_path / "my_data_package" / "test_folder2").mkdir()
    (tmp_path / "my_data_package" / "test_folder3").mkdir()
    (tmp_path / "my_data_package" / "test_folder3" / "test_folder3").mkdir()
    (
        tmp_path / "my_data_package" / "test_folder3" / "test_folder3" / "test_folder3"
    ).mkdir()
    (tmp_path / "my_data_package" / "test_folder1" / "text.txt").touch()
    (tmp_path / "my_data_package" / "test_folder2" / "random").touch()
    (tmp_path / "my_data_package" / "test_folder2" / ".hidden").touch()
    (tmp_path / "my_data_package" / "script.py").touch()
    return tmp_path / "my_data_package"


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
def ckan_url():
    return os.environ.get("CKAN_URL")


@pytest.fixture()
def ckan_api():
    return os.environ.get("CKAN_API")


@pytest.fixture()
def ckan_package_name():
    return os.environ.get("TEST_PACKAGE_NAME")
