import json
import pathlib

import ckanapi

from ..utilities import get_secret

"""
API Documentation:
https://docs.ckan.org/en/2.9/api/index.html#action-api-reference
"""


class CKAN:
    def __init__(self, server: str, apikey: str=None, secret: str=None, verify_certificate: bool=True):
        self.server = server
        self.apikey = apikey if apikey is not None else get_secret(secret)
        self.verify = verify_certificate

    def connection(self):
        return ckanapi.RemoteCKAN(self.server, apikey=self.apikey)

    def get_package_metadata(self, package_name: str, filter_fields: list = None):
        """
        filter_fields: list,
            will filter the requested data to only return specified fields.
            eg: ["maintainer", "author", "usage_contact", "timerange", "notes", "spatial", "private", "num_tags", "tags", "tags_string"]
        """

        with self.connection() as conn:
            data = conn.call_action(
                "package_show",
                data_dict={"id": package_name},
                requests_kwargs={"verify": self.verify}
            )

        if filter_fields is not None:
            return {k: v for k, v in data.items() if k in filter_fields}

        return data

    def update_package_metadata(self, package_data: dict):
        """You must provide the full metadata"""
        with self.connection() as conn:
            return conn.call_action(
                "package_update",
                data_dict=package_data,
                requests_kwargs={"verify": self.verify}
            )

    def patch_package_metadata(self, package_name: str, package_data_to_update: dict):
        """You provide only the key-value-pairs you want to update"""
        all_metadata_of_package = self.get_package_metadata(package_name)
        all_metadata_of_package.update(package_data_to_update)
        return self.update_package_metadata(all_metadata_of_package)

    def update_package_from_file(self, package_data_file: str | pathlib.Path):
        if isinstance(package_data_file, str):
            package_data_file = pathlib.Path(package_data_file)

        if not package_data_file.exists():
            raise FileNotFoundError(f"The file '{package_data_file.absolute().as_posix()}' can not be found.")

        with package_data_file.open() as meta:
            metadata = json.load(meta)

        return self.update_package_metadata(metadata)


