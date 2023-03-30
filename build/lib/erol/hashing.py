import hashlib
import pathlib
from functools import partial
from typing import Callable


def import_hash_func(hash_func_name: str):
    try:
        return getattr(hashlib, hash_func_name)
    except AttributeError:
        raise ImportError(
            f"Unfortunately hashing via '{hash_func_name}' is not implemented in python's hashlib library"
        )


def _hash(file: pathlib.Path, hash_func: Callable, block_size: int = 65536):
    """
    From python3.11 there's a native implementation which is marginally faster.
    https://docs.python.org/3/library/hashlib.html#hashlib.file_digest
    """
    hf = hash_func()
    with file.open("rb") as f:
        chunk = f.read(block_size)
        while chunk:
            hf.update(chunk)
            chunk = f.read(block_size)
    return hf.hexdigest()


def get_hash_func(hash_func_name: str, block_size: int = 65536):
    hash_func = import_hash_func(hash_func_name)
    return partial(_hash, hash_func=hash_func, block_size=block_size)
