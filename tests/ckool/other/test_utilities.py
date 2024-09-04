from ckool.other.utilities import upload_via_api, extract_resource_id


def test_upload_via_api():
    assert upload_via_api(
        [1024, 1024**2, 10 * 1024**2],
        100 * 1024**2,
        False,
    )
    assert upload_via_api([1024**2], 2 * 1024**2 + 1, False, factor=2)
    assert not upload_via_api([1024**2], 5 * 1024**2, False, factor=5)
    assert not upload_via_api([1024**2], 1024**2, False, factor=1)
    assert upload_via_api([1024**2, 1024**2], 1024**2 + 1, False, factor=1)
    assert not upload_via_api([1024**2, 1024**2], 1024**2 + 1, True, factor=1)
    assert not upload_via_api([1024**2], 1, True, factor=1)


def test_extract_resource_id():
    assert extract_resource_id("0b6955ef-0d8a-4fed-a2b3-196185321d6d-scripts.zip") == "0b6955ef-0d8a-4fed-a2b3-196185321d6d"
    assert extract_resource_id("abc") == "abc"
    assert extract_resource_id("abc0b6955ef-0d8a-4fed-a2b3-196185321d6d-scripts.zip") == "abc0b6955ef-0d8a-4fed-a2b3-196185321d6d-scripts.zip"