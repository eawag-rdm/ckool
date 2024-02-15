import json
import pathlib



def _write_cache(meta: dict, cache_file: pathlib.Path):
    with cache_file.open("w+") as cache:
        json.dump(meta, cache)
    return cache_file


def read_cache(cache_file: pathlib.Path):
    with cache_file.open() as cache:
        return json.load(cache)


def update_cache(meta: dict, cache_file: pathlib.Path):
    if not cache_file.parent.exists():
        cache_file.parent.mkdir(exist_ok=True, parents=True)
    if not cache_file.exists():
        return _write_cache(meta, cache_file)
    else:
        read_meta = read_cache(cache_file)
        read_meta.update(meta)
        return _write_cache(read_meta, cache_file)
