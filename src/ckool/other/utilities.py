import os
import pathlib
import sys
from functools import wraps
from subprocess import PIPE, CalledProcessError, run

from ckool.other.types import HashTypes


def get_secret(name):
    """first checking the 'environment variables' for the secret then checking 'pass' the password manager."""
    secret = os.environ.get(name)
    if secret is not None:
        return secret

    try:
        proc = run(["pass", name], stdout=PIPE, stderr=PIPE)
        proc.check_returncode()
    except CalledProcessError:
        sys.exit(
            f"ERROR: Could not find '{name}' in 'pass' password-store or 'env' variables."
        )
    else:
        secret = proc.stdout.decode("utf-8")
        if "\n" in secret:
            secret = secret.split("\n")[0]
        return secret


def partial(func, /, *args, **keywords):
    # the default implementation does not use wraps, I need it
    @wraps(func)
    def new_func(*f_args, **f_keywords):
        new_keywords = {**keywords, **f_keywords}
        return func(*args, *f_args, **new_keywords)

    new_func.func = func
    new_func.args = args
    new_func.keywords = keywords
    return new_func


def upload_via_api(
    file_sizes,
    space_available_on_server_root_disk,
    parallel_upload,
    factor: float = 4.8,
):
    """
    This function is used to decide, if data should be uploaded via the API or via the hacky way.
    The hacky way requires uploading an empty resource followed by replacing the empty resource
    with the actual file via SCP and updating the resource metadata.

    During the upload via API and caching of multiple systems (uwsgi, nginx, python, ...)
    each uploaded resource will quadruple in size (the factor parameter implements this behaviour),
    especially for parallel uploads this should be considered to not fill up the root disk and crash the server
    """

    combined_size = sum(file_sizes)
    max_single_size = max(file_sizes)

    max_file_size = space_available_on_server_root_disk / factor

    if any(
        [
            parallel_upload and combined_size < max_file_size,
            not parallel_upload and max_single_size < max_file_size,
        ]
    ):
        return True
    return False


class DataIntegrityError(Exception):
    pass


# TODO: not in use, can be gotten rid of
def enrich_resource_metadata(
    pkg_name: str,
    filename: pathlib.Path,
    hash_string: str,
    file_size: int = None,
    resource_type: str = None,
    hashtype: str = "sha256",
):
    return {
        "package_id": pkg_name,
        "citation": "",
        "name": filename.name,
        "resource_type": resource_type or "Dataset",
        "url": "dummy",
        "restricted_level": "public",
        "hashtype": hashtype,
        "hash": hash_string,
        "size": file_size or filename.stat().st_size,
    }


def collect_metadata(file: pathlib.Path, hash_: str, hashtype: HashTypes):
    return {
        "file": file.as_posix(),
        "hash": hash_,
        "hashtype": hashtype.value,
        "size": file.stat().st_size,
        "format": file.suffix[1:],  # erasing the point from suffix
    }


def resource_is_link(resource_metadata: dict):
    link = True
    if (
        resource_metadata["url_type"] == "upload"
        and pathlib.Path(resource_metadata["url"]).parent.name == "download"
    ):  # the resource is a link
        link = False
    return link
