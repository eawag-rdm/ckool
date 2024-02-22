import concurrent.futures
import json
import pathlib

import ckanapi
import requests

from ckool import EMPTY_FILE_NAME
from ckool.ckan.upload import upload_resource
from ckool.other.types import HashTypes
from ckool.other.utilities import get_secret

"""
API Documentation:
https://docs.ckan.org/en/2.9/api/index.html#action-api-reference
"""


def get_resource_key(resources: list, package_name: str, resource_name: str):
    names, ids = [d["name"] for d in resources], [d["id"] for d in resources]

    if len(set(names)) != len(names) and resource_name in names:
        raise ValueError(
            f"The resource name you provided '{resource_name}' is not unique in the package '{package_name}'. "
            f"Please use the a resource id."
        )
    return "name" if resource_name in names else "id"


def resource_name_to_id(resources: list, package_name: str, resource_name_or_id: str):
    names, ids = [d["name"] for d in resources], [d["id"] for d in resources]

    if len(set(names)) != len(names) and resource_name_or_id in names:
        raise ValueError(
            f"The resource name you provided '{resource_name_or_id}' is not unique in the package '{package_name}'. "
            f"Please use the a resource id."
        )
    if resource_name_or_id in names:
        return [r["id"] for r in resources if r["name"] == resource_name_or_id][0]
    elif resource_name_or_id in ids:
        return resource_name_or_id
    else:
        raise ValueError(
            f"Invalid resource_id or resource_name. The provided identifier '{resource_name_or_id}'"
            f" is not present in the package '{package_name}'."
        )


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
        self.token = token if token else get_secret(secret_token)
        self.verify = verify_certificate

    def connection(self):
        return ckanapi.RemoteCKAN(self.server, apikey=self.token)

    def resolve_resource_id_or_name_to_id(self, package_name, resource_id_or_name):
        resources = self.get_package(package_name)["resources"]
        return {
            "id": resource_name_to_id(resources, package_name, resource_id_or_name),
            "resources": resources,
        }

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

        if kwargs.get("include_private", None) is None:
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
        self, package_name, resource_id_or_name, ckan_storage_path=""
    ):
        resource_id = self.resolve_resource_id_or_name_to_id(
            package_name, resource_id_or_name
        )["id"]

        rsc_1, rsc_2, rsc_3 = resource_id[:3], resource_id[3:6], resource_id[6:]
        local_resource_path = f"{rsc_1}/{rsc_2}/{rsc_3}"

        if ckan_storage_path.endswith("/"):
            ckan_storage_path = ckan_storage_path[:-1]

        if ckan_storage_path:
            if not ckan_storage_path.endswith("resources"):
                ckan_storage_path += "/resources"
            ckan_storage_path += "/"
        return f"{ckan_storage_path}{local_resource_path}"

    def get_resource_meta(self, package_name, resource_id_or_name):
        resolved = self.resolve_resource_id_or_name_to_id(
            package_name, resource_id_or_name
        )
        return [r for r in resolved["resources"] if r["id"] == resolved["id"]][0]

    def get_project(self, project_name):
        return self.plain_action_call(
            "group_show", id=project_name, include_datasets=True
        )

    def get_organization(self, organization_name):
        return self.plain_action_call("organization_show", id=organization_name)

    def get_user(self, username):
        return self.plain_action_call("user_show", id=username)

    def get_vocabulary(self):
        return self.plain_action_call("vocabulary_list")

    def create_project(self, **kwargs):
        return self.plain_action_call("group_create", **kwargs)

    def create_organization(self, **kwargs):
        return self.plain_action_call("organization_create", **kwargs)

    def create_package(self, **kwargs):
        return self.plain_action_call("package_create", **kwargs)

    def create_resource_of_type_link(self, **kwargs):
        return self.plain_action_call("resource_create", **kwargs)

    def create_resource_of_type_file(
        self,
        file: str | pathlib.Path,
        package_id: str,
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
        progressbar: int = True,
    ):
        if isinstance(file, str):
            file = pathlib.Path(file)

        return upload_resource(
            file_path=file,
            package_id=package_id,
            ckan_url=self.server,
            api_key=self.token,
            hash=hash,
            size=size,
            citation=citation,
            description=description,
            format=format,
            name=name,
            hashtype=hashtype,
            resource_type=resource_type,
            restricted_level=restricted_level,
            state=state,
            verify=self.verify,
            progressbar=progressbar,
        )

    def update_package_metadata(self, package_data: dict):
        """You must provide the full metadata"""
        return self.plain_action_call("package_update", **package_data)

    def patch_resource_metadata(self, resource_id: str, resource_data_to_update: dict):
        """You must provide the full metadata"""
        resource_data_to_update.update({"id": resource_id})
        return self.plain_action_call("resource_patch", **resource_data_to_update)

    def patch_empty_resource_name(
        self,
        package_name: str,
        new_resource_name: str,
        emtpy_resource_name: str = EMPTY_FILE_NAME,
    ):
        resolved = self.resolve_resource_id_or_name_to_id(
            package_name, emtpy_resource_name
        )
        return self.patch_resource_metadata(
            resource_id=resolved["id"],
            resource_data_to_update={"name": new_resource_name},
        )

    def patch_package_metadata(self, package_id: str, data: dict):
        data.update({"id": package_id})
        return self.plain_action_call("package_patch", **data)

    def patch_user(self, user_id: str, data: dict):
        data.update({"id": user_id})
        return self.plain_action_call("user_patch", **data)

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

    def reorder_package_resources(self, package_name, reverse=False):
        resources = self.get_package(
            package_name=package_name, filter_fields=["resources"]
        )["resources"]
        resource_ids = sorted(
            [(r["id"], r["name"]) for r in resources],
            reverse=reverse,
            key=lambda x: x[1],
        )
        return self._update_package_resource_reorder(
            package_name, [r[0] for r in resource_ids]
        )

    def update_linked_resource_url(self, resource_id, url):
        return self.plain_action_call("resource_patch", id=resource_id, url=url)

    def delete_resource(self, resource_id):
        return self.plain_action_call("resource_delete", id=resource_id)

    def delete_project(self, project_id):
        return self.plain_action_call("group_delete", id=project_id)

    def delete_package(self, package_id):
        return self.plain_action_call("package_delete", id=package_id)

    def delete_organization(self, organization_id):
        return self.plain_action_call("organization_delete", id=organization_id)

    def purge_organization(self, organization_id):
        return self.plain_action_call("organization_purge", id=organization_id)

    def purge_project(self, group_id):
        return self.plain_action_call("group_purge", id=group_id)

    def add_package_to_project(self, package_name, project_name):
        self.patch_package_metadata(
            package_id=package_name, data={"groups": [{"name": project_name}]}
        )

    def delete_all_resources_from_package(self, package_name):
        pkg = self.get_package(package_name)
        resource_ids = [resource["id"] for resource in pkg["resources"]]
        return [self.delete_resource(resource_id) for resource_id in resource_ids]

    def download_resource(
        self,
        url: str,
        destination: str | pathlib.Path,
        chunk_size=8192,
    ):
        return _download_resource(url, self.token, destination, chunk_size, self.verify)

    def _download_link_sequentially(
        self,
        links: list,
        destination: pathlib.Path,
        chunk_size=8192,
    ):
        done = []
        for link in links:
            name = pathlib.Path(link).name
            done.append(self.download_resource(link, destination / name, chunk_size))
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
                verify=self.verify,
            )
        else:
            files = self._download_link_sequentially(
                resources_to_download,
                destination,
                chunk_size=chunk_size,
            )

        return files + [destination / "metadata.json"]


def filter_resources(
    package_metadata: dict,
    resources_to_exclude: list,
    always_to_exclude_restriction_levels=None,
):
    if always_to_exclude_restriction_levels is None:
        always_to_exclude_restriction_levels = ["only_allowed_users"]

    resources = package_metadata["resources"]

    resources_filtered = []
    resource_map = {resource["id"]: resource["name"] for resource in resources}

    error_message = (
        f"You may not provide a mixture of resource ids and resource names. You must use one or the other. "
        f"You provided:\n{','.join(resources_to_exclude)}\n"
        f"Package resources are:\n{json.dumps(resource_map, indent=2)}"
    )

    identifier_provided = None
    for resource_to_exclude in resources_to_exclude:
        found = False

        if resource_to_exclude in resource_map.keys():
            found = True
            if identifier_provided == "name":
                raise ValueError(error_message)
            identifier_provided = "id"

        if resource_to_exclude in resource_map.values():
            found = True
            if identifier_provided == "id":
                raise ValueError(error_message)
            identifier_provided = "name"

        if not found:
            raise ValueError(
                f"The resource you provided to be excluded '{resource_to_exclude}' does not exist."
            )

    if (
        len(resource_map.values()) != len(set(resource_map.values()))
        and identifier_provided == "name"
    ):
        resource_names = sorted(list(resource_map.values()))
        duplicates = set(
            [
                resource_names[i]
                for i in range(len(resource_names))
                if resource_names[i - 1] == resource_names[i]
            ]
        )
        raise ValueError(
            f"There are multiple resources with the same name '{duplicates}', please provide resource_ids."
        )

    for resource in resources:
        if resource["restricted_level"] in always_to_exclude_restriction_levels:
            continue
        if resource[identifier_provided] in resources_to_exclude:
            continue
        resources_filtered.append(resource)

    pmd = package_metadata.copy()
    pmd["resources"] = resources_filtered
    return pmd
