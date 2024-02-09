import os
import pathlib
import sys
from functools import wraps
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
    files, space_available_on_server_root_disk, parallel_upload, factor: int = 4.8
):
    """
    This function is used to decide, if data should be uploaded via the API or via the hacky way.
    The hacky way requires uploading an empty resource followed by replacing the empty resource
    with the actual file via SCP and updating the resource metadata.

    During the upload via API and caching of multiple systems (uwsgi, nginx, python, ...)
    each uploaded resource will quadruple in size (the factor parameter implements this behaviour),
    especially for parallel uploads this should be considered to not fill up the root disk and crash the server
    """

    sizes = [file["size"] for file in files]
    combined_size = sum(sizes)
    max_single_size = max(sizes)

    max_file_size = space_available_on_server_root_disk / factor

    if any(
        [
            parallel_upload and combined_size < max_file_size,
            not parallel_upload and max_single_size < max_file_size,
        ]
    ):
        return True
    return False


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
