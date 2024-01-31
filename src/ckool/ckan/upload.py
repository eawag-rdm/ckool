import pathlib

import requests
from requests_toolbelt.multipart.encoder import (
    MultipartEncoder,
    MultipartEncoderMonitor,
)
from tqdm import tqdm


class TqdmProgressCallback:
    def __init__(self, total_size):
        self.total_size = total_size
        self.progress_bar = tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Uploading"
        )

    def __call__(self, monitor):
        self.progress_bar.update(monitor.bytes_read - self.progress_bar.n)

    def close(self):
        self.progress_bar.close()
        print("Waiting for CKAN server!")


def upload_resource(
    file_path: pathlib.Path,
    package_id: str,
    ckan_url: str,
    api_key: str,
    file_hash: str,
    file_size: int,
    citation: str = "",
    description: str = "",
    format: str = "",
    hashtype: str = "sha256",
    resource_type: str = "Dataset",
    restricted_level: str = "public",
    state: str = "active",
    allow_insecure: bool = False,
):
    """
    {
        "citation": "",
        "description": "",
        "format": "",
        "hash": "db033c4634a43bfbca7ea7c1cf21d76dfd23d53bde1c20fbf953e6fe358600d5",
        "hashtype": "sha256",
        "name": "20230224_N1MHV_02150221_11.ncr",
        "resource_type": "Dataset",
        "restricted_level": "public",
        "size": 511757238,
        "state": "active"
    }
    """
    file_name = file_path.name
    print(file_name)
    with open(file_path, "rb") as file_stream:
        encoder = MultipartEncoder(
            fields={
                "upload": (file_name, file_stream, "application/octet-stream"),
                "package_id": package_id,
                "name": file_name,
                "mimetype": "application/octet-stream",
                "citation": citation,
                "description": description,
                "format": format,
                "hash": file_hash,
                "hashtype": hashtype,
                "state": state,
                "size": str(file_size),
                "resource_type": resource_type,
                "restricted_level": restricted_level,
            }
        )

        progress_callback = TqdmProgressCallback(file_size)
        monitor = MultipartEncoderMonitor(encoder, progress_callback)

        headers = {"Authorization": api_key, "Content-Type": monitor.content_type}

        return requests.post(
            f"{ckan_url}/api/3/action/resource_create",
            data=monitor,
            headers=headers,
            auth=None,
            stream=True,
            verify=not allow_insecure,
        )
