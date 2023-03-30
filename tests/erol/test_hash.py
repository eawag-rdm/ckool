from hashlib import sha256

import pyinstrument
import pytest

from erol.hashing import _hash, get_hash_func, import_hash_func


def test_hash_speed(large_file):
    profiler = pyinstrument.Profiler()
    profiler.start()
    _hash(large_file, sha256)
    profiler.stop()
    profiler.print()


@pytest.mark.parametrize("hash_func_name", ["md5", "sha256", "sha384"])
def test_import_hash_func(hash_func_name):
    import_hash_func(hash_func_name)


def test_import_hash_func_raises():
    with pytest.raises(ImportError):
        import_hash_func("abc")


def test_get_hash_func(small_file):
    this_hash = get_hash_func("sha256")
    b = _hash(file=small_file, hash_func=sha256)
    a = this_hash(small_file)
    assert b == a
