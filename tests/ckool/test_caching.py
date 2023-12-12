from ckool.caching import read_cache, update_cache, write_cache


def test_write_cache(data_to_cache, cache_file):
    write_cache(data_to_cache, cache_file)
    assert cache_file.exists()


def test_read_cache(data_to_cache, cache_file):
    write_cache(data_to_cache, cache_file)
    data = read_cache(cache_file)
    assert data == data_to_cache


def test_update_cache(data_to_cache, cache_file):
    write_cache(data_to_cache, cache_file)
    update_cache({"hello": "there", "hash_type": None}, cache_file)
    data = read_cache(cache_file)
    assert data.get("hello") == "there"
    assert data.get("hash_type") is None
