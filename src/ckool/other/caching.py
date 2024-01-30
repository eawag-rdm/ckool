import json
import pathlib


def write_cache(meta: dict, cache_file: pathlib.Path):
    with cache_file.open("w+") as cache:
        json.dump(meta, cache)


def read_cache(cache_file: pathlib.Path):
    with cache_file.open() as cache:
        return json.load(cache)


def update_cache(meta: dict, cache_file: pathlib.Path):
    read_meta = read_cache(cache_file)
    read_meta.update(meta)
    write_cache(read_meta, cache_file)
