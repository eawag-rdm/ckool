import pytest

from ckool.interfaces.interfaces import SecureInterface
from ckool.other.hashing import get_hash_func

md5 = get_hash_func("md5")


@pytest.mark.impure
def test_secure_interface_init(config_internal):
    SecureInterface(**config_internal["ckan_server"][0])

    with pytest.raises(FileNotFoundError):
        SecureInterface(
            host="FakeHost", username="SomeUserName", ssh_key="/does/not/exist/id_rsa"
        )

    with pytest.raises(ValueError):
        SecureInterface(host="FakeHost", username="SomeUserName")
        SecureInterface(
            host="FakeHost",
            username="SomeUserName",
            secret_password="abs",
            ssh_key="ass",
        )
        SecureInterface(
            host="FakeHost",
            username="SomeUserName",
            secret_password="abs",
            ssh_key="ass",
            secret_passphrase="dsfs",
        )
        SecureInterface(
            host="FakeHost",
            username="SomeUserName",
            secret_password="abs",
            secret_passphrase="ass",
        )
        SecureInterface(
            host="FakeHost", username="SomeUserName", secret_passphrase="ass"
        )


@pytest.mark.impure
def test_ssh(config_internal):
    si = SecureInterface(**config_internal["ckan_server"][0])
    out, err = si.ssh("ls /home")
    users = [i for i in out.split("\n") if i]
    assert si.username in users


@pytest.mark.impure
def test_scp(tmp_path, config_internal):
    file = tmp_path / "abc"
    si = SecureInterface(**config_internal["ckan_server"][0])

    file.touch()
    file.write_text("test")

    si.scp(file, "/tmp/test_file")
    assert md5(file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]


@pytest.mark.slow
@pytest.mark.impure
def test_scp_large_with_progress(tmp_path, config_internal, large_file):
    si = SecureInterface(**config_internal["ckan_server"][0])

    si.scp(large_file, "/tmp/test_file")
    print(si.ssh("md5sum /tmp/test_file"))
    assert md5(large_file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]


@pytest.mark.slow
@pytest.mark.impure
def test_scp_large_without_progress(tmp_path, config_internal, large_file):
    si = SecureInterface(**config_internal["ckan_server"][0])

    si.scp(large_file, "/tmp/test_file", progressbar=False)
    print(si.ssh("md5sum /tmp/test_file"))
    assert md5(large_file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]
