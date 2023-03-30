import pathlib


def meta_default(
        pkg_name: str,
        filename: pathlib.Path,
        hash_string: str,
        file_size: int = None,
        resource_type: str = None,
        hash_type: str = "sha256"
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
        "size": file_size or filename.stat().st_size
    }

