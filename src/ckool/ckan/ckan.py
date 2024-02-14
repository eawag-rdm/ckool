import concurrent.futures
import json
import pathlib

import ckanapi
import requests

from ckool.ckan.upload import upload_resource
from ckool.other.utilities import get_secret

"""
API Documentation:
https://docs.ckan.org/en/2.9/api/index.html#action-api-reference
"""


def _download_resource(
    url: str,
    api_key: str,
    destination_file_path: str | pathlib.Path,
    chunk_size: int = 8192,
    verify: bool = True,
):
    with requests.get(
        url, headers={"X-CKAN-API-Key": api_key}, stream=True, verify=verify
    ) as req:
        req.raise_for_status()
        with open(destination_file_path, "wb") as f:
            for chunk in req.iter_content(chunk_size=chunk_size):
                f.write(chunk)
    return destination_file_path


def _wrapper_for_parallel(args):
    """ignores chunk_size for now"""
    url, api_key, destination_file_path, chunk_size, verify = args
    return _download_resource(url, api_key, destination_file_path, chunk_size, verify)


class CKAN:
    def __init__(
        self,
        server: str,
        token: str = None,
        secret_token: str = None,
        verify_certificate: bool = True,
    ):
        self.server = server
        self.token = token if token is not None else get_secret(secret_token)
        self.verify = verify_certificate

    def connection(self):
        return ckanapi.RemoteCKAN(self.server, apikey=self.token)

    def plain_action_call(self, endpoint, **kwargs):
        with self.connection() as conn:
            return conn.call_action(
                endpoint,
                data_dict=kwargs,
                requests_kwargs={"verify": self.verify},
            )

    def get_all_packages(self, **kwargs):
        """
        https://docs.ckan.org/en/2.9/api/#ckan.logic.action.get.package_search
        """

        if not kwargs.get("include_private"):
            kwargs["include_private"] = True
        if not kwargs.get("rows"):
            kwargs["rows"] = 1000

        return self.plain_action_call("package_search", **kwargs)

    def get_package(self, package_name: str, filter_fields: list = None):
        """
        filter_fields: list,
            will filter the requested data to only return specified fields.
            eg: ["maintainer", "author", "usage_contact", "timerange", "notes", "spatial", "private", "num_tags", "tags", "tags_string"]
        """

        data = self.plain_action_call("package_show", id=package_name)

        if filter_fields is not None:
            return {k: v for k, v in data.items() if k in filter_fields}

        return data

    def get_local_resource_path(
        self, package_name, resource_name, ckan_storage_path=""
    ):
        data = self.get_package(package_name, filter_fields=["resources"])

        resource = [r for r in data["resources"] if r["name"] == resource_name][0]
        resource_id = resource.get("id")

        rsc_1, rsc_2, rsc_3 = resource_id[:3], resource_id[3:6], resource_id[6:]
        local_resource_path = f"{rsc_1}/{rsc_2}/{rsc_3}"

        if ckan_storage_path.endswith("/"):
            ckan_storage_path = ckan_storage_path[:-1]

        if ckan_storage_path:
            if not ckan_storage_path.endswith("resources"):
                ckan_storage_path += "/resources"
            ckan_storage_path += "/"
        return f"{ckan_storage_path}{local_resource_path}"

    def get_resource_meta(self, package_name, resource_name):
        data = self.get_package(package_name, filter_fields=["resources"])
        return [r for r in data["resources"] if r["name"] == resource_name][0]

    def get_project(self, project_name):
        return self.plain_action_call("group_show", id=project_name)

    def get_user(self, username):
        return self.plain_action_call("group_show", id=username)

    def create_organization(self, **kwargs):
        """CURL example

        curl --insecure -X POST https://localhost:8443/api/3/action/organization_create \
         -H "Content-Type: application/json" \
         -H "Authorization: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJEV1dtMzhzQ2ZkMk1ob2V5SmZ5ZjJ0dHRjOERIYjdEQ0pPUU93QXNSdGFYYjdDbC12THRtRlhFVTg5ZExGU096aUxWTjAxeXlvSUYwSEdLQiIsImlhdCI6MTcwNjE4NzEzMn0.3ki03QvUSKDi61cug2ooD0WL-ckVzwhnIIV1UlrgCAo" \
         -d '{
               "name": "test_organization",
               "title": "Test_Organization",
               "description": "This is my organization.",
               "homepage": "https://www.eawag.ch/de/",
               "datamanager": "ckan_admin",
               "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg"
             }'
        """
        return self.plain_action_call("organization_create", **kwargs)

    def create_package(self, **kwargs):
        """CURL example

        curl --insecure -X POST "https://localhost:8443/api/3/action/package_create" \
         -H "Content-Type: application/json" \
         -H "Authorization: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJEV1dtMzhzQ2ZkMk1ob2V5SmZ5ZjJ0dHRjOERIYjdEQ0pPUU93QXNSdGFYYjdDbC12THRtRlhFVTg5ZExGU096aUxWTjAxeXlvSUYwSEdLQiIsImlhdCI6MTcwNjE4NzEzMn0.3ki03QvUSKDi61cug2ooD0WL-ckVzwhnIIV1UlrgCAo" \
         -d '{
               "name": "test_package",
               "title": "Test_Package",
               "private": false,
               "description": "This is my package.",
               "author": "ckan_admin",
               "author_email": "your_email@example.com",
               "state": "active",
               "type": "dataset",
               "owner_org": "test_organization",
               "reviewed_by": "",
               "maintainer": "ckan_admin",
               "usage_contact": "ckan_admin",
               "notes": "some_note",
               "review_level": "none",
               "spatial": "{\"type\": \"Point\", \"coordinates\": [8.609776496939471, 47.40384502816517]}",
               "status": "incomplete",
               "tags_string": "some_tag",
               "timerange": "*",
               "variables": "none"
             }'
        """
        return self.plain_action_call("package_create", **kwargs)

    def create_resource_of_type_link(self, **kwargs):
        """CURL example

        curl --insecure -X POST "https://localhost:8443/api/3/action/resource_create" \
         -H "Content-Type: application/json" \
         -H "Authorization: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJjdDdKUWNKMUkzaXFSRGUtVFUzd0x4eG1fWjMyWkFwZG53eU80ejh3d1dRd240ZkljcEh0UXpBS0RJSWVsUUhuMU92NHRNU0dLNVZncWdHNyIsImlhdCI6MTcwNjE3NDc3OH0.Qvz3CSL9lmYtboxfMTw5Pa6vOCttNuqpPYIcih5nvig" \
         -d '{
               "package_id": "test_package",
               "name": "test_resource",
               "resource_type": "Dataset",
               "restricted_level": "public",
               "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg"
             }'

        This does not work for files, as the ckanapi package is currently broken.
        """
        return self.plain_action_call("resource_create", **kwargs)

    def create_resource_of_type_file(
        self,
        file: str | pathlib.Path,
        package_id: str,
        file_hash: str,
        file_size: int,
        citation: str = "",
        description: str = "",
        file_format: str = "",
        hash_type: str = "sha256",
        resource_type: str = "Dataset",
        restricted_level: str = "public",
        state: str = "active",
        progressbar: int = True,
    ):
        if isinstance(file, str):
            file = pathlib.Path(file)

        return upload_resource(
            file_path=file,
            package_id=package_id,
            ckan_url=self.server,
            api_key=self.token,
            file_hash=file_hash,
            file_size=file_size,
            citation=citation,
            description=description,
            file_format=file_format,
            hash_type=hash_type,
            resource_type=resource_type,
            restricted_level=restricted_level,
            state=state,
            verify=self.verify,
            progressbar=progressbar,
        )

    def update_package_metadata(self, package_data: dict):
        """You must provide the full metadata"""
        return self.plain_action_call("package_update", **package_data)

    def patch_package_metadata(self, package_name: str, package_data_to_update: dict):
        """You provide only the key-value-pairs you want to update"""
        all_metadata_of_package = self.get_package(package_name)
        all_metadata_of_package.update(package_data_to_update)
        return self.update_package_metadata(all_metadata_of_package)

    def update_package_from_file(self, package_data_file: str | pathlib.Path):
        if isinstance(package_data_file, str):
            package_data_file = pathlib.Path(package_data_file)

        if not package_data_file.exists():
            raise FileNotFoundError(
                f"The file '{package_data_file.absolute().as_posix()}' can not be found."
            )

        with package_data_file.open() as meta:
            metadata = json.load(meta)

        return self.update_package_metadata(metadata)

    def update_doi(self, package_name, doi, citation):
        """Inserts DOI and citation (retrieved from DataCite) into
        target-host metadata. Use from ckool.interfaces.mixed_requests import get_citation_from_doi
        """
        return self.plain_action_call(
            "package_patch", id=package_name, doi=doi, citation=citation
        )

    def _update_package_resource_reorder(self, package_id, resource_ids: list):
        return self.plain_action_call(
            "package_resource_reorder", id=package_id, order=resource_ids
        )

    def reorder_package_resources(self, package_id, reverse=False):
        resources = self.get_package(
            package_name=package_id, filter_fields=["resources"]
        )["resources"]
        resource_ids = sorted(
            [(r["id"], r["name"]) for r in resources],
            reverse=reverse,
            key=lambda x: x[1],
        )
        return self._update_package_resource_reorder(
            package_id, [r[0] for r in resource_ids]
        )

    def update_linked_resource_url(self, resource_id, url):
        return self.plain_action_call("resource_patch", id=resource_id, url=url)

    def delete_resource(self, resource_id):
        return self.plain_action_call("resource_delete", id=resource_id)

    def delete_package(self, package_id):
        return self.plain_action_call("package_delete", id=package_id)

    def delete_organization(self, organization_id):
        return self.plain_action_call("organization_delete", id=organization_id)

    def purge_organization(self, organization_id):
        return self.plain_action_call("organization_purge", id=organization_id)

    def delete_all_resources_from_package(self, package_name):
        pkg = self.get_package(package_name)
        resource_ids = [resource["id"] for resource in pkg["resources"]]
        return [self.delete_resource(resource_id) for resource_id in resource_ids]

    def download_resource(
        self,
        url: str,
        destination_file_path: str | pathlib.Path,
        chunk_size=8192,
        verify: bool = True,
    ):
        return _download_resource(
            url, self.token, destination_file_path, chunk_size, verify
        )

    def _download_link_sequentially(
        self,
        links: list,
        destination: pathlib.Path,
        chunk_size=8192,
        verify: bool = True,
    ):
        done = []
        for link in links:
            name = pathlib.Path(link).name
            done.append(
                self.download_resource(link, destination / name, chunk_size, verify)
            )
        return done

    @staticmethod
    def _download_resources_in_parallel(
        links, api_key, destination, max_workers, chunk_size=8192, verify=True
    ):
        # prepare_args
        n = len(links)
        _list_api_key = [api_key for _ in range(n)]
        _list_destination = [
            destination / pathlib.Path(link).name for _, link in zip(range(n), links)
        ]
        _chunk_size = [chunk_size for _ in range(n)]
        _verify = [verify for _ in range(n)]

        done = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(
                _wrapper_for_parallel,
                zip(links, _list_api_key, _list_destination, _chunk_size, _verify),
            ):
                done.append(result)
        return done

    def download_package_with_resources(
        self,
        package_name: str,
        destination: str | pathlib.Path,
        parallel: bool = False,
        max_workers: int = None,
        chunk_size: int = 8192,
        verify: bool = True,
    ):
        """
        Significant performance differences can be seen for many large resources.
        """
        # TODO write tests for this
        if isinstance(destination, str):
            destination = pathlib.Path(destination)

        metadata = self.get_package(package_name)
        with (destination / "metadata.json").open("w") as f:
            json.dump(metadata, f)

        resources_to_download = [
            res["url"] for res in metadata["resources"] if res["url_type"] == "upload"
        ]

        if parallel:
            files = self._download_resources_in_parallel(
                resources_to_download,
                self.token,
                destination,
                max_workers=max_workers,
                chunk_size=chunk_size,
                verify=verify,
            )
        else:
            files = self._download_link_sequentially(
                resources_to_download, destination, chunk_size=chunk_size, verify=verify
            )

        return files + [destination / "metadata.json"]
