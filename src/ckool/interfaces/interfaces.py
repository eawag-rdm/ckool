import pathlib
import sys
from dataclasses import dataclass

from paramiko import AutoAddPolicy, SSHClient
from scp import SCPClient


def to_pathlib(path: str | pathlib.Path):
    return path if isinstance(path, pathlib.Path) else pathlib.Path(path)


@dataclass
class SecureInterface:
    """scp and ssh via paramiko"""

    host: str
    username: str
    port: int = 22
    password: str = None
    ssh_key: str = None
    passphrase: str = None

    def __check_input(self):
        if any(
            [
                self.password and self.ssh_key and self.passphrase,
                self.password and self.ssh_key and self.passphrase is None,
                self.password and self.ssh_key is None and self.passphrase,
                self.password is None and self.ssh_key is None and self.passphrase,
                self.password is None
                and self.ssh_key is None
                and self.passphrase is None,
            ]
        ):
            raise ValueError(
                f"Your set of input arguments is invalid. You can provide either only a password, only an ssh-key or an "
                f"ssh-key and it's passphrase (if set). Your input arguments: '{repr(locals())}'"
            )

        if (
            self.ssh_key
            and not (key := pathlib.Path(self.ssh_key)).absolute().exists()
            or not key.is_file()
        ):
            raise FileNotFoundError(
                f"The provided ssh-key '{self.ssh_key}' can not be accessed. Does it exist? Read permissions?"
            )

    def __post_init__(self):
        self.__check_input()

    def _get_ssh_client(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            passphrase=self.passphrase,
            key_filename=self.ssh_key,
        )
        return ssh

    def scp(
        self,
        local_filepath: str | pathlib.Path,
        remote_filepath: str | pathlib.Path,
        show_progress: bool = False,
    ):
        """To copy to remote host only"""
        local_filepath = to_pathlib(local_filepath)
        remote_filepath = to_pathlib(remote_filepath)

        def progress4(filename, size, sent, peername):
            sys.stdout.write(
                "(%s:%s) %s's progress: %.2f%%   \r"
                % (peername[0], peername[1], filename, float(sent) / float(size) * 100)
            )

        with SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                passphrase=self.passphrase,
                key_filename=self.ssh_key,
            )

            kwargs = {} if not show_progress else {"progress4": progress4}
            with SCPClient(ssh.get_transport(), **kwargs) as scp:
                scp.put(local_filepath, remote_filepath.as_posix())

        # TODO: add recursive uploads
        # Uploading the 'test' directory with its content in the
        # '/home/user/dump' remote directory
        # scp.put('test', recursive=True, remote_path='/home/user/dump')

    def ssh(self, command):
        with SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                passphrase=self.passphrase,
                key_filename=self.ssh_key,
            )

            stdin, stdout, stderr = ssh.exec_command(command)
            out, err = stdout.read().decode("utf8"), stderr.read().decode("utf8")

            # Get return code from command (0 is default for success)
            # print(f"Return code: {stdout.channel.recv_exit_status()}")

            return out, err
