import pathlib

import requests
from requests_toolbelt.multipart.encoder import (
    MultipartEncoder,
    MultipartEncoderMonitor,
)
from tqdm.auto import tqdm

from ckool.other.types import HashTypes


class TqdmProgressCallback:
    def __init__(self, total_size, filename, progressbar: int = True):
        position = None
        global position_queue
        if "position_queue" in globals():
            position = position_queue.get_nowait()

        self.total_size = total_size
        self.filename = filename
        self.progressbar = progressbar
        self.bar = tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=f"Uploading (API) {filename}",
            disable=not progressbar,
            position=position,
        )

    def __call__(self, monitor):
        self.bar.update(monitor.bytes_read - self.bar.n)
        self.bar.refresh()

    def close(self):
        self.bar.close()


def upload_resource(
    file_path: pathlib.Path,
    package_id: str,
    ckan_url: str,
    api_key: str,
    hash: str,
    size: int,
    citation: str = "",
    description: str = "",
    format: str = "",
    name: str = None,
    hashtype: HashTypes = HashTypes.sha256,
    resource_type: str = "Dataset",
    restricted_level: str = "public",
    state: str = "active",
    verify: bool = True,
    progressbar: int = True,
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
    with open(file_path, "rb") as file_stream:
        encoder = MultipartEncoder(
            fields={
                "upload": (
                    file_name,
                    file_stream,
                    format if format else "application/octet-stream",
                ),
                "package_id": package_id,
                "name": file_name if name is None else name,
                # "mimetype": "application/octet-stream",  # this overwrites the format
                "citation": citation,
                "description": description,
                "format": format,
                "hash": hash,
                "hashtype": hashtype if isinstance(hashtype, str) else hashtype.value,
                "state": state,
                "size": str(size),
                "resource_type": resource_type,
                "restricted_level": restricted_level,
            }
        )

        progress_callback = TqdmProgressCallback(size, file_name, progressbar)
        monitor = MultipartEncoderMonitor(encoder, progress_callback)

        headers = {"Authorization": api_key, "Content-Type": monitor.content_type}

        response = requests.post(
            f"{ckan_url}/api/3/action/resource_create",
            data=monitor,
            headers=headers,
            auth=None,
            stream=True,
            verify=verify,
        )

        progress_callback.close()

        return response
