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
    resource_type: str = "Dataset",
    restricted_level: str = "public",
    allow_insecure: bool = False,
):
    file_name = file_path.name
    file_size = file_path.stat().st_size

    with open(file_path, "rb") as file_stream:
        encoder = MultipartEncoder(
            fields={
                "upload": (file_name, file_stream, "application/octet-stream"),
                "package_id": package_id,
                "name": file_name,
                "mimetype": "application/octet-stream",
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
