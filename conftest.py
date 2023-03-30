import os
import pathlib

import pytest

from erol.interface_remote import RemoteInterface


def generate_file(size: int, path: pathlib.Path, name: str):
    file_path = path / name
    with file_path.open("wb") as f:
        f.write(os.urandom(size))
    yield file_path
    file_path.unlink()


@pytest.fixture
def small_file(tmp_path):
    """100KB"""
    yield from generate_file(1024 * 10**2, tmp_path, "small.bin")


@pytest.fixture
def large_file(tmp_path):
    """100MB"""
    yield from generate_file(1024 * 10**5, tmp_path, "large.bin")


@pytest.fixture
def very_large_file(tmp_path):
    """5GB"""
    yield from generate_file(512 * 10**7, tmp_path, "very_large.bin")


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


@pytest.fixture
def remote_interface():
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIxUVRGZk9ub3F0QzNkQmNVUE1xWUhoVzNoc3ZjbnRuSjFxU1ZoUkdsMVJjIiwiaWF0IjoxNjc5OTA5NzY3fQ.C-FjyA4UJbs2Z0Fbtv5aRW9DsgE0QGygxiWgNYtfQwk"
    with RemoteInterface("http://159.89.215.168", apikey=token) as remote:
        yield remote
