import pathlib
from dataclasses import dataclass

from paramiko import AutoAddPolicy, SSHClient
from scp import SCPClient
from tqdm.auto import tqdm

from ckool.other.utilities import get_secret


def to_pathlib(path: str | pathlib.Path):
    return path if isinstance(path, pathlib.Path) else pathlib.Path(path)


@dataclass
class SecureInterface:
    """scp and ssh via paramiko"""

    host: str
    username: str
    port: int = 22
    secret_password: str = None
    ssh_key: str = None
    secret_passphrase: str = None

    def __check_input(self):
        if any(
            [
                self.secret_password and self.ssh_key and self.secret_passphrase,
                self.secret_password and self.ssh_key and not self.secret_passphrase,
                self.secret_password and not self.ssh_key and self.secret_passphrase,
                not self.secret_password
                and not self.ssh_key
                and self.secret_passphrase,
                not self.secret_password
                and not self.ssh_key
                and not self.secret_passphrase,
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
            password=get_secret(self.secret_password),
            passphrase=get_secret(self.secret_passphrase),
            key_filename=self.ssh_key,
        )
        return ssh

    def scp(
        self,
        local_filepath: str | pathlib.Path,
        remote_filepath: str | pathlib.Path,
        progressbar: bool = True,
    ):
        """To copy to remote host only"""
        local_filepath = to_pathlib(local_filepath)
        remote_filepath = to_pathlib(remote_filepath)

        position = None
        global position_queue
        if "position_queue" in globals():
            position = position_queue.get_nowait()
        print([i for i in globals().keys() if "q" in i])
        print(position)
        pbar = tqdm(
            total=local_filepath.stat().st_size,
            unit="B",
            unit_scale=True,
            desc=f"Uploading (SCP) {local_filepath.name}",
            disable=not progressbar,
            position=position,
        )

        def progress(filename, size, sent, peername):
            pbar.update(sent - pbar.n)
            pbar.set_postfix_str("%.2f%%" % (float(sent) / float(size) * 100))

        with SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            ssh.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.secret_password,
                passphrase=self.secret_passphrase,
                key_filename=self.ssh_key,
            )

            kwargs = {"progress4": progress} if progressbar else {}
            with SCPClient(ssh.get_transport(), **kwargs) as scp:
                return scp.put(local_filepath, remote_filepath.as_posix())

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
                password=self.secret_password,
                passphrase=self.secret_passphrase,
                key_filename=self.ssh_key,
            )

            stdin, stdout, stderr = ssh.exec_command(command)
            out, err = stdout.read().decode("utf8"), stderr.read().decode("utf8")

            # Get return code from command (0 is default for success)
            # print(f"Return code: {stdout.channel.recv_exit_status()}")

            return out, err
