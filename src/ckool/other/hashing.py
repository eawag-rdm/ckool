import hashlib
import pathlib
from typing import Callable

from tqdm.auto import tqdm

from ckool.other.utilities import partial


def import_hash_func(hash_func_name: str):
    try:
        return getattr(hashlib, hash_func_name)
    except AttributeError:
        raise ImportError(
            f"Unfortunately hashing via '{hash_func_name}' is not implemented in python's hashlib library"
        )


def _hash(
    filepath: pathlib.Path,
    hash_func: Callable,
    block_size: int = 65536,
    progressbar_position: int = None,
):
    """
    From python3.11 there's a native implementation which is marginally faster.
    https://docs.python.org/3/library/hashlib.html#hashlib.file_digest
    """
    hf = hash_func()
    iterations = filepath.stat().st_size / block_size
    progressbar = tqdm(
        total=int(iterations) + 1,
        desc=f"Hashing {filepath.name}",
        position=progressbar_position,
    )

    with filepath.open("rb") as f:
        chunk = f.read(block_size)
        progressbar.update()
        while chunk:
            hf.update(chunk)
            chunk = f.read(block_size)
            progressbar.update()
            progressbar.refresh()

    progressbar.close()

    return hf.hexdigest()


def get_hash_func(hash_func_name: str, block_size: int = 65536):
    hash_func = import_hash_func(hash_func_name)
    return partial(_hash, hash_func=hash_func, block_size=block_size)
