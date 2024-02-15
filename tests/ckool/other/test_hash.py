from hashlib import sha256

import pyinstrument
import pytest

from ckool import HASH_TYPE
from ckool.other.hashing import _hash, get_hash_func, import_hash_func
from ckool.other.types import HashTypes


def test_hash_speed(large_file):
    profiler = pyinstrument.Profiler()
    profiler.start()
    _hash(large_file, sha256)
    profiler.stop()
    profiler.print()


@pytest.mark.parametrize(
    "hash_func_name",
    [h.value for h in HashTypes],
)
def test_import_hash_func(hash_func_name):
    import_hash_func(hash_func_name)


def test_import_hash_func_raises():
    with pytest.raises(ImportError):
        import_hash_func("abc")


def test_get_hash_func(small_file):
    this_hash = get_hash_func(HASH_TYPE)
    b = _hash(filepath=small_file, hash_func=sha256)
    a = this_hash(small_file)
    assert b == a


def test_get_hash_func_test_progressbar(small_file):
    _hash(filepath=small_file, hash_func=sha256, progressbar=True)
    _hash(filepath=small_file, hash_func=sha256, progressbar=False)


def test_get_hash_func_larger(large_file):
    this_hash = get_hash_func(HASH_TYPE)
    b = _hash(filepath=large_file, hash_func=sha256)
    a = this_hash(large_file)
    assert b == a
