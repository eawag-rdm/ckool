from ckool.other.utilities import upload_via_api


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
