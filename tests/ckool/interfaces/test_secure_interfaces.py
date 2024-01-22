import pytest

from ckool.hashing import get_hash_func
from ckool.interfaces.interfaces import SecureInterface

md5 = get_hash_func("md5")


def test_secure_interface_init(secure_interface_input_args):
    SecureInterface(**secure_interface_input_args)

    with pytest.raises(FileNotFoundError):
        SecureInterface(
            host="FakeHost", username="SomeUserName", ssh_key="/does/not/exist/id_rsa"
        )

    with pytest.raises(ValueError):
        SecureInterface(host="FakeHost", username="SomeUserName")
        SecureInterface(
            host="FakeHost", username="SomeUserName", password="abs", ssh_key="ass"
        )
        SecureInterface(
            host="FakeHost",
            username="SomeUserName",
            password="abs",
            ssh_key="ass",
            passphrase="dsfs",
        )
        SecureInterface(
            host="FakeHost", username="SomeUserName", password="abs", passphrase="ass"
        )
        SecureInterface(host="FakeHost", username="SomeUserName", passphrase="ass")


def test_ssh(secure_interface_input_args):
    si = SecureInterface(**secure_interface_input_args)
    out, err = si.ssh("ls /home")
    users = [i for i in out.split("\n") if i]
    assert si.username in users


def test_scp(tmp_path, secure_interface_input_args):
    file = tmp_path / "abc"
    si = SecureInterface(**secure_interface_input_args)

    file.touch()
    file.write_text("test")

    si.scp(file, "/tmp/test_file")
    assert md5(file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]
