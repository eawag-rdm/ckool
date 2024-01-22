import json
import pathlib

import ckanapi
import requests

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

    def get_package(self, package_name: str, filter_fields: list = None):
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

    def get_project(self, project_name):
        with self.connection() as conn:
            return conn.call_action(
                "group_show",
                data_dict={"id": project_name},
                requests_kwargs={"verify": self.verify}
            )

    def get_user(self, username):
        with self.connection() as conn:
            return conn.call_action(
                "group_show",
                data_dict={"id": username},
                requests_kwargs={"verify": self.verify}
            )

    def download_resource(self, url: str, destination_file_path: str | pathlib.Path, chunk_size=8192):
        with requests.get(url, headers={"X-CKAN-API-Key": self.apikey}, stream=True) as req:
            req.raise_for_status()
            with open(destination_file_path, "wb") as f:
                for chunk in req.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        return destination_file_path

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
        all_metadata_of_package = self.get_package(package_name)
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

    def update_doi(self, package_name, doi, citation):
        """Inserts DOI and citation (retrieved from DataCite) into
        target-host metadata. Use from ckool.interfaces.mixed_requests import get_citation_from_doi"""
        with self.connection() as conn:
            return conn.call_action(
                "package_patch",
                data_dict={"id": package_name, "doi": doi, "citation": citation},
                requests_kwargs={"verify": self.verify}
            )

    def update_linked_resource_url(self, resource_id, url):
        with self.connection() as conn:
            return conn.call_action(
                "resource_patch",
                data_dict={"id": resource_id, "url": url},
                requests_kwargs={"verify": self.verify}
            )

    def delete_all_resources_from_package(self, package_name):
        pkg = self.get_package(package_name)
        resource_ids = [resource["id"] for resource in pkg["resources"]]
        for resource_id in resource_ids:
            with self.connection() as conn:
                return conn.call_action(
                    "resource_delete",
                    data_dict={"id": resource_id,},
                    requests_kwargs={"verify": self.verify}
                )
