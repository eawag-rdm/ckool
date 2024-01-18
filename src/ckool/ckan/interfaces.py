import pathlib
from dataclasses import dataclass

import paramiko
from ckanapi import RemoteCKAN

"""
API Documentation:
https://docs.ckan.org/en/2.10/api/index.html#action-api-reference
"""


class RemoteCKANInterface(RemoteCKAN):
    """Wrapper around RemoteCKAN from ckanapi"""

    pass


@dataclass
class RemoteHostInterface:
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
                self.password is None
                and self.ssh_key is None
                and self.passphrase is None,
                self.password and self.ssh_key and self.passphrase,
                self.password and self.ssh_key is None and self.passphrase,
                self.password is None and self.ssh_key is None and self.passphrase,
            ]
        ):
            raise ValueError(
                "Your set of input arguments is invalid. You can provide either only a password, only an ssh-key or an ssh-key and it's passphrase."
            )

        if (
            not (key := pathlib.Path(self.ssh_key)).absolute().exists()
            or not key.is_file()
        ):
            raise ValueError(
                f"The provided ssh-key '{self.ssh_key}' can not be accessed. Does it exist? Read permissions?"
            )

    def scp(self, local_file_path, remote_file_path):
        pass

    def ssh(self, command):
        pass


def abc():
    command = "df"

    # Update the next three lines with your
    # server's information

    host = "YOUR_IP_ADDRESS"
    username = "YOUR_LIMITED_USER_ACCOUNT"
    password = "YOUR_PASSWORD"

    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password)
    _stdin, _stdout, _stderr = client.exec_command("df")
    print(_stdout.read().decode())
    client.close()

    from paramiko import AutoAddPolicy, SSHClient

    client = SSHClient()
    # client.load_system_host_keys()
    # client.load_host_keys('~/.ssh/known_hosts')
    # client.set_missing_host_key_policy(AutoAddPolicy())

    client.connect(
        "example.com",
        username="user",
        key_filename="mykey.pem",
        passphrase="mysshkeypassphrase",
    )
    client.close()

    from paramiko import SSHClient

    # Connect
    client = SSHClient()
    client.load_system_host_keys()
    client.connect("example.com", username="user", password="secret")

    # Run a command (execute PHP interpreter)
    stdin, stdout, stderr = client.exec_command("php")
    print(type(stdin))  # <class 'paramiko.channel.ChannelStdinFile'>
    print(type(stdout))  # <class 'paramiko.channel.ChannelFile'>
    print(type(stderr))  # <class 'paramiko.channel.ChannelStderrFile'>

    # Optionally, send data via STDIN, and shutdown when done
    stdin.write('<?php echo "Hello!"; sleep(2); ?>')
    stdin.channel.shutdown_write()

    # Print output of command. Will wait for command to finish.
    print(f'STDOUT: {stdout.read().decode("utf8")}')
    print(f'STDERR: {stderr.read().decode("utf8")}')

    # Get return code from command (0 is default for success)
    print(f"Return code: {stdout.channel.recv_exit_status()}")

    # Because they are file objects, they need to be closed
    stdin.close()
    stdout.close()
    stderr.close()

    # Close the client itself
    client.close()
