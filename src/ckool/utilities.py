import os
import pathlib
import sys
from subprocess import PIPE, CalledProcessError, run


def get_secret(name):
    """first checking the 'environment variables' for the secret then checking 'pass' the password manager."""
    secret = os.environ.get(name)
    if secret is not None:
        return secret

    try:
        proc = run(["pass", name], stdout=PIPE, stderr=PIPE)
        proc.check_returncode()
    except CalledProcessError:
        sys.exit("ERROR: Didn't find {} in .password-store either")
    else:
        secret = proc.stdout.decode("utf-8").strip("\n")
        return secret


def meta_default(
    pkg_name: str,
    filename: pathlib.Path,
    hash_string: str,
    file_size: int = None,
    resource_type: str = None,
    hash_type: str = "sha256",
):
    return {
        "package_id": pkg_name,
        "citation": "",
        "name": filename.name,
        "resource_type": resource_type or "Dataset",
        "url": "dummy",
        "restricted_level": "public",
        "hashtype": hash_type,
        "hash": hash_string,
        "size": file_size or filename.stat().st_size,
    }
