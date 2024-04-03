import json
import multiprocessing
import os
import pathlib
import queue
import re
from contextlib import contextmanager

import ckanapi
import pytest
from tests.ckool.data.inputs.ckan_entity_data import (
    organization_data,
    package_data,
    project_data,
    resource_data,
)

from ckool import HashTypes
from ckool.ckan.ckan import CKAN
from ckool.ckan.upload import upload_resource
from ckool.datacite.datacite import DataCiteAPI

ckan_instance_names_of_fixtures = [
    pytest.param(
        "ckan_instance",
    ),
    pytest.param("ckan_open_instance", marks=pytest.mark.open),
]


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
        "--run-open",
        action="store_true",
        default=False,
        help="run tests that require ERIC open",
    )
    parser.addoption(
        "--run-impure", action="store_true", default=False, help="run impure tests"
    )
    parser.addoption(
        "--run-dora",
        action="store_true",
        default=False,
        help="run tests, that require dora",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks test as slow")
    config.addinivalue_line(
        "markers", "open: marks test as requiring ERIC open test instance"
    )
    config.addinivalue_line(
        "markers", "impure: marks test as impure (depending on other systems)"
    )
    config.addinivalue_line(
        "markers",
        "dora: marks test as dependant on dora, which require to be in the Eawag network",
    )


def pytest_collection_modifyitems(config, items):
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    skip_impure = pytest.mark.skip(reason="need --run-impure option to run")
    skip_dora = pytest.mark.skip(reason="need --run-dora option to run")
    skip_open = pytest.mark.skip(reason="need --run-open option to run")

    if (
        config.getoption("--run-slow")
        and config.getoption("--run-impure")
        and config.getoption("--run-dora")
        and config.getoption("--run-open")
    ):
        return

    for item in items:
        if "slow" in item.keywords and not config.getoption("--run-slow"):
            item.add_marker(skip_slow)
        elif "impure" in item.keywords and not config.getoption("--run-impure"):
            item.add_marker(skip_impure)
        elif "dora" in item.keywords and not config.getoption("--run-dora"):
            item.add_marker(skip_dora)
        elif "open" in item.keywords and not config.getoption("--run-open"):
            item.add_marker(skip_open)


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
def ckan_entities(load_env_file):
    return {
        "test_package": os.environ.get("CKAN_PACKAGE_NAME") or "test_package",
        "test_organization": os.environ.get("CKAN_ORGANIZATION_NAME")
        or "test_organization",
        "test_project": os.environ.get("CKAN_GROUP_NAME") or "test_group",
        "test_resource": os.environ.get("CKAN_RESOURCE_NAME") or "test_resource",
    }


@pytest.fixture
def config(tmp_path, load_env_file):
    def _config(internal: bool):
        string = "INTERNAL"
        if not internal:
            string = "OPEN"
        return {
            "other": [
                {
                    "space_available_on_server_root_disk": 10 * 1024**3,
                    "ckan_storage_path": os.environ.get(f"{string}_CKAN_STORAGE_PATH"),
                    "datamanager": "" if string == "INTERNAL" else "eawrdmadmin",
                }
            ],
            "ckan_api": [
                {
                    "server": os.environ.get(f"{string}_CKAN_URL"),
                    "token": os.environ.get(f"{string}_CKAN_TOKEN"),
                    "secret_token": "",
                }
            ],
            "ckan_server": [
                {
                    "host": os.environ.get(f"{string}_SECURE_INTERFACE_HOST"),
                    "port": os.environ.get(f"{string}_SECURE_INTERFACE_PORT"),
                    "username": os.environ.get(f"{string}_SECURE_INTERFACE_USERNAME"),
                    "ssh_key": os.environ.get(f"{string}_SECURE_INTERFACE_SSH_KEY"),
                    "secret_passphrase": "",
                    "secret_password": "",
                }
            ],
            "datacite": {
                "user": os.environ["DATACITE_USER"],
                "password": os.environ["DATACITE_PASSWORD"],
                "prefix": os.environ["DATACITE_PREFIX"],
                "host": os.environ["DATACITE_URL"],
                "offset": os.environ["DATACITE_OFFSET"],
            },
            "local_doi_store_path": tmp_path,
        }

    return _config


def add_instance_names(cfg: dict, name: str):
    for section in ["other", "ckan_api", "ckan_server"]:
        for entry in range(len(cfg[section])):
            cfg[section][entry]["instance"] = name
    return cfg


@pytest.fixture
def config_internal(tmp_path, load_env_file, config):
    return config(internal=True)


@pytest.fixture
def ckan_instance_name_internal(config_internal):
    return "test_instance"


@pytest.fixture
def ckan_instance_name_open(config_internal):
    return "test_instance_open"


@pytest.fixture
def config_section_instance_internal(config_internal, ckan_instance_name_internal):
    config_internal = add_instance_names(config_internal, ckan_instance_name_internal)
    return {
        "config": {"Test": config_internal},
        "section": "Test",
        "ckan_instance_name": ckan_instance_name_internal,
    }


@pytest.fixture
def config_open(tmp_path, load_env_file, config):
    return config(internal=False)


@pytest.fixture
def config_section_instance_open(config_open, ckan_instance_name_open):
    config_open = add_instance_names(config_open, ckan_instance_name_open)
    return {
        "config": {"Test": config_open},
        "section": "Test",
        "ckan_instance_name": ckan_instance_name_open,
    }


@pytest.fixture
def full_config(
    config_open, config_internal, ckan_instance_name_internal, ckan_instance_name_open
):
    config_internal = add_instance_names(config_internal, ckan_instance_name_internal)
    config_open = add_instance_names(config_open, ckan_instance_name_open)
    return {
        "Test": {
            "other": [config_internal["other"][0], config_open["other"][0]],
            "ckan_api": [config_internal["ckan_api"][0], config_open["ckan_api"][0]],
            "ckan_server": [
                config_internal["ckan_server"][0],
                config_open["ckan_server"][0],
            ],
            "datacite": config_internal["datacite"],
            "local_doi_store_path": config_internal["local_doi_store_path"],
        }
    }


@pytest.fixture
def ckan_instance(load_env_file):
    return CKAN(
        server=os.environ.get("INTERNAL_CKAN_URL"),
        token=os.environ.get("INTERNAL_CKAN_TOKEN"),
        verify_certificate=False,
    )


@pytest.fixture
def ckan_open_instance(load_env_file):
    return CKAN(
        server=os.environ.get("OPEN_CKAN_URL"),
        token=os.environ.get("OPEN_CKAN_TOKEN"),
        verify_certificate=False,
    )


def patch_user(ckan_instance):
    user_record = ckan_instance.get_user("ckan_admin")
    if not user_record["fullname"]:
        ckan_instance.patch_user(
            user_id=user_record["id"], data={"fullname": "ckan admin"}
        )


def setup(ckan_instance, ckan_entities):
    patch_user(ckan_instance)

    organization_data.update({"name": ckan_entities["test_organization"]})
    package_data.update({"name": ckan_entities["test_package"]})

    resource_data.update(
        {
            "package_id": ckan_entities["test_package"],
            "name": ckan_entities["test_resource"],
        }
    )
    project_data.update({"name": ckan_entities["test_project"]})
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


def teardown(ckan_instance, ckan_entities):
    packages = ckan_instance.get_all_packages()["results"]
    for package in packages:
        if package["organization"]["name"] != ckan_entities["test_organization"]:
            continue
        ckan_instance.delete_package(package["id"])
        ckan_instance.purge_package(package["id"])
    try:
        ckan_instance.delete_project(ckan_entities["test_project"])
        ckan_instance.purge_project(ckan_entities["test_project"])
    except ckanapi.NotFound:
        pass
    try:
        ckan_instance.delete_organization(ckan_entities["test_organization"])
        ckan_instance.purge_organization(ckan_entities["test_organization"])
    except ckanapi.NotFound:
        pass


@contextmanager
def managed_ckan_setup(ckan_instance: CKAN, ckan_entities, run_setup=True):
    try:
        if run_setup:
            setup(ckan_instance, ckan_entities)
        yield
    finally:
        teardown(ckan_instance, ckan_entities)


@contextmanager
def managed_doi_setup(tmp_path, datacite_instance, ckan_entities):
    doi = "10.5524/123C45"
    try:
        datacite_instance.doi_reserve(doi)

        (pkg_dir := tmp_path / "name-1" / ckan_entities["test_package"]).mkdir(
            parents=True
        )
        (pkg_dir / "doi.txt").write_text(doi)
        yield
    finally:
        datacite_instance.doi_delete(doi)


@pytest.fixture
def ckan_setup_data(ckan_instance, ckan_entities):
    with managed_ckan_setup(ckan_instance, ckan_entities, run_setup=True):
        yield


@pytest.fixture
def ckan_open_cleanup(ckan_open_instance, ckan_entities):
    with managed_ckan_setup(ckan_open_instance, ckan_entities, run_setup=False):
        yield


@pytest.fixture
def doi_setup(tmp_path, datacite_instance, ckan_entities):
    with managed_doi_setup(tmp_path, datacite_instance, ckan_entities):
        yield


def detect_ckan_instance(request):
    test_name = request.node.name
    fixture_name = re.search(r"\[(.*?)\]", test_name).group(1)
    return [f for f in fixture_name.split("-") if "ckan" in f and "instance" in f][0]


@pytest.fixture(scope="function")
def dynamic_ckan_instance(request):
    fixture_name = detect_ckan_instance(request)
    instance = request.getfixturevalue(fixture_name)
    yield instance


@pytest.fixture
def dynamic_ckan_setup_data(request, dynamic_ckan_instance, ckan_entities):
    with managed_ckan_setup(dynamic_ckan_instance, ckan_entities):
        yield


@pytest.fixture
def dynamic_config(request, config):
    fixture_name = detect_ckan_instance(request)
    return config(internal="open" not in fixture_name.lower())


@pytest.fixture
def dynamic_config_section_instance(request, config):
    fixture_name = detect_ckan_instance(request)
    return {
        "config": {
            "Test": add_instance_names(
                config(internal="open" not in fixture_name.lower()), "test_instance"
            )
        },
        "section": "Test",
        "ckan_instance_name": "test_instance",
    }


@pytest.fixture()
def add_file_resources(tmp_path, ckan_entities):
    def _add_file_resources(ckan_instance: CKAN, package_sizes: list):
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
                    package_id=ckan_entities["test_package"],
                    ckan_url=ckan_instance.server,
                    api_key=ckan_instance.token,
                    size=file.stat().st_size,
                    hash="fake-hash-to-save-time",
                    hashtype=HashTypes.md5,
                    resource_type="Dataset",
                    restricted_level="public",
                    verify=False,
                )
            files.append(file)

        for f in files:
            f.unlink()

    return _add_file_resources


@pytest.fixture
def datacite_instance(load_env_file):
    return DataCiteAPI(
        user=os.environ["DATACITE_USER"],
        password=os.environ["DATACITE_PASSWORD"],
        prefix=os.environ["DATACITE_PREFIX"],
        host=os.environ["DATACITE_URL"],
        offset=os.environ["DATACITE_OFFSET"],
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
def pretty_large_file(tmp_path):
    """1GB"""
    yield from generate_binary_file(1 * 1024**3, tmp_path, "pretty_large.bin")


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

    for idx, size in enumerate(file_sizes):
        files.append(_generate_binary_file(size, root_folder, f"large_{idx}.bin"))

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


def function_wrapper(func, return_queue, args, kwargs):
    """Run the function and put its result in a queue."""
    result = func(*args, **kwargs)
    return_queue.put(result)


@pytest.fixture
def run_with_timeout():
    def _run_with_timeout(func, timeout, *args, **kwargs):
        # Create a Queue to share results between processes
        return_queue = multiprocessing.Queue()

        # Wrap the function call in a Process
        process = multiprocessing.Process(
            target=function_wrapper, args=(func, return_queue, args, kwargs)
        )
        process.start()
        process.join(timeout)

        if process.is_alive():
            # If the process is still alive after the timeout, terminate it
            process.terminate()
            process.join()
            print("Aborting function execution.")
        else:
            # Otherwise, get the result from the queue
            try:
                return return_queue.get_nowait()
            except queue.Empty:
                return

    return _run_with_timeout
