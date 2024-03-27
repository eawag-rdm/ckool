import hashlib
import pathlib
from typing import Callable

from tqdm import tqdm

from ckool.other.types import HashTypes
from ckool.other.utilities import partial


def import_hash_func(hash_func_name: str | HashTypes):
    try:
        return getattr(
            hashlib,
            hash_func_name if isinstance(hash_func_name, str) else hash_func_name.value,
        )
    except AttributeError:
        raise ImportError(
            f"Unfortunately hashing via '{hash_func_name}' is not implemented in python's hashlib library"
        )


def _hash(
    filepath: pathlib.Path,
    hash_func: Callable,
    block_size: int = 65536,
    progressbar: bool = True,
):
    """
    From python3.11 there's a native implementation which is marginally faster.
    https://docs.python.org/3/library/hashlib.html#hashlib.file_digest
    """
    hf = hash_func()

    position = None
    global position_queue
    if "position_queue" in globals():
        position = position_queue.get()

    bar = tqdm(
        total=filepath.stat().st_size,
        desc=f"Hashing {filepath.name}",
        disable=not progressbar,
        position=position,
        unit="B",
        unit_scale=True,
    )

    with filepath.open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            bar.update(len(chunk))  # Update progress bar based on the size of the chunk read
            hf.update(chunk)

    bar.close()

    return hf.hexdigest()


def get_hash_func(hash_func_name: HashTypes | str, block_size: int = 65536):
    hash_func = import_hash_func(hash_func_name)
    return partial(_hash, hash_func=hash_func, block_size=block_size)
