import pytest

from ckool.interfaces.interfaces import SecureInterface
from ckool.other.hashing import get_hash_func

md5 = get_hash_func("md5")


@pytest.mark.impure
def test_secure_interface_init(secure_interface_input_args):
    SecureInterface(**secure_interface_input_args)

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
def test_ssh(secure_interface_input_args):
    si = SecureInterface(**secure_interface_input_args)
    out, err = si.ssh("ls /home")
    users = [i for i in out.split("\n") if i]
    assert si.username in users


@pytest.mark.impure
def test_scp(tmp_path, secure_interface_input_args):
    file = tmp_path / "abc"
    si = SecureInterface(**secure_interface_input_args)

    file.touch()
    file.write_text("test")

    si.scp(file, "/tmp/test_file")
    assert md5(file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]


@pytest.mark.slow
@pytest.mark.impure
def test_scp_large_with_progress(tmp_path, secure_interface_input_args, large_file):
    si = SecureInterface(**secure_interface_input_args)

    si.scp(large_file, "/tmp/test_file")
    print(si.ssh("md5sum /tmp/test_file"))
    assert md5(large_file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]


@pytest.mark.slow
@pytest.mark.impure
def test_scp_large_without_progress(tmp_path, secure_interface_input_args, large_file):
    si = SecureInterface(**secure_interface_input_args)

    si.scp(large_file, "/tmp/test_file", progressbar=False)
    print(si.ssh("md5sum /tmp/test_file"))
    assert md5(large_file) == si.ssh("md5sum /tmp/test_file")[0].split(" ")[0]
