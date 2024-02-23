import json
import pathlib
import shutil
import sys

from rich.prompt import Prompt

from ckool import (
    DOWNLOAD_CHUNK_SIZE,
    HASH_BLOCK_SIZE,
    PACKAGE_META_DATA_FILE_ENDING,
    TEMPORARY_DIRECTORY_NAME,
    UPLOAD_FUNC_FACTOR,
)
from ckool.ckan.ckan import CKAN
from ckool.ckan.publishing import (
    create_resource_raw,
    enrich_and_store_metadata,
    patch_package_raw,
    patch_resource_metadata_raw,
    publish_datacite_doi,
    update_datacite_doi, create_package_raw,
)
from ckool.datacite.datacite import DataCiteAPI
from ckool.datacite.doi_store import LocalDoiStore
from ckool.interfaces.mixed_requests import get_citation_from_doi
from ckool.other.caching import read_cache
from ckool.other.config_parser import config_for_instance, find_target_ckan_instance, parse_config_for_use
from ckool.other.file_management import get_compression_func, iter_package
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.other.utilities import resource_is_link
from ckool.parallel_runner import map_function_with_threadpool
from ckool.templates import (
    create_resource_raw_wrapped,
    get_upload_func,
    handle_file,
    handle_folder,
    handle_missing_entities,
    handle_resource_download_with_integrity_check,
    handle_upload,
    hash_remote,
    resource_integrity_between_ckan_instances_intact,
    retrieve_and_filter_source_metadata,
)


# TODO adding additional resource metadata fields how? Maybe via file
#  Resource should be in alphabetical order how to set the order?
def _upload_package(
    package_name: str,
    package_folder: str,
    include_sub_folders: bool,
    compression_type: CompressionTypes,
    include_pattern: str,
    exclude_pattern: str,
    hash_algorithm: HashTypes,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    Example calls here:

    """

    section = "Production" if not test else "Test"
    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if not parallel:
        for info in iter_package(
            package_folder,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                handle_file(
                    file,
                    hash_func,
                    hash_algorithm,
                    tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )

            elif folder := info["folder"]:  # folders are archived and then hashed
                if not include_sub_folders:
                    continue

                handle_folder(
                    folder,
                    hash_func,
                    compression_func,
                    hash_algorithm,
                    tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content:\n{repr(info)}"
                )

        handle_upload(
            package_name,
            package_folder,
            config,
            section,
            ckan_instance,
            verify,
            parallel,
            progressbar=True,
        )
    else:
        raise NotImplementedError("Parallel will be implemented soon.")


def _upload_resource(
    package_name: str,
    filepath: str,
    hash_algorithm: HashTypes,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    filepath = pathlib.Path(filepath)
    hash_func = get_hash_func(hash_algorithm)

    if filepath.is_file():
        handle_file(
            filepath,
            hash_func,
            hash_algorithm,
            tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
            block_size=HASH_BLOCK_SIZE,
            progressbar=True,
        )

    else:
        raise ValueError(
            f"The filepath your specified '{filepath.as_posix()}' is not a file."
        )

    handle_upload(
        package_name,
        filepath.parent,
        config,
        section,
        ckan_instance,
        verify,
        parallel=False,
        progressbar=True,
    )


def _prepare_package(
    package_folder: str,
    include_sub_folders: bool,
    include_pattern: str,
    exclude_pattern: str,
    compression_type: CompressionTypes,
    hash_algorithm: HashTypes,
    parallel: bool,
    ignore_prepared: bool,
):
    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if ignore_prepared:
        shutil.rmtree(package_folder / TEMPORARY_DIRECTORY_NAME)

    if not parallel:
        for info in iter_package(
            package_folder,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                handle_file(
                    file,
                    hash_func,
                    hash_algorithm,
                    tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            elif folder := info["folder"]:  # folders are archived and then hashed
                if not include_sub_folders:
                    continue

                handle_folder(
                    folder,
                    hash_func,
                    compression_func,
                    hash_algorithm,
                    tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content: '{repr(info)}'"
                )


def _get_local_resource_location(
    package_name: str,
    resource_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    storage_path = config_for_instance(config[section]["other"], ckan_instance)[
        "ckan_storage_path"
    ]
    ckan = CKAN(**cfg_ckan_api)
    print(
        ckan.get_local_resource_path(
            package_name=package_name,
            resource_id_or_name=resource_name,
            ckan_storage_path=storage_path,
        )
    )


def _download_package(
    package_name: str,
    destination: str,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    return (
        ckan.download_package_with_resources(
            package_name=package_name,
            destination=destination,
            parallel=parallel,
            max_workers=None,
            chunk_size=chunk_size,
        ),
    )


def _download_resource(
    url: str,
    destination: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    """
    Example calls here
    """
    destination = pathlib.Path(destination)
    if destination.exists():
        destination = destination / pathlib.Path(url).name

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    ckan.download_resource(
        url=url,
        destination=destination,
        chunk_size=chunk_size,
    )

    return


def _download_resources(
    url_file: str,
    destination_folder: str,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    destination = pathlib.Path(destination_folder)
    url_file = pathlib.Path(url_file)
    with url_file.open("r") as urls:
        urls_to_download = [url for url in urls]

    if parallel:
        map_function_with_threadpool(
            _download_resource,
            [
                [
                    url,
                    destination.as_posix(),
                    config,
                    ckan_instance,
                    verify,
                    test,
                    chunk_size,
                ]
                for url in urls_to_download
            ],
        )
    else:
        for url in urls:
            _download_resource(
                url,
                destination.as_posix(),
                config,
                ckan_instance,
                verify,
                test,
                chunk_size,
            )


def _download_metadata(
    package_name: str,
    filter_fields: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    if filter_fields is not None:
        filter_fields = filter_fields.split(",")

    ckan = CKAN(**cfg_ckan_api)
    print(
        json.dumps(
            ckan.get_package(package_name=package_name, filter_fields=filter_fields),
            indent=4,
        )
    )


def _download_all_metadata(
    include_private,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    result = ckan.get_all_packages(include_private=include_private)
    if result["count"] == 1000:
        raise Warning(
            "The maximal numbers of rows were retrieved. "
            "Please check the ckanapi documentation for the package_search function "
            "on how to implement retrieval of more rows."
        )
    print(json.dumps(result, indent=4))


def _patch_package(
    metadata_file: str,
    package_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    should be interactive, show diff, let user select what to patch
    probably needs its own implementation independent of ckanapi
    """
    raise NotImplementedError("Parallel will be implemented soon.")
    # section = "Production" if not test else "Test"
    # cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    # cfg_ckan_api.update({"verify_certificate": verify})
    # ckan = CKAN(**cfg_ckan_api)
    # metadata_in_ckan = ckan.get_package(package_name)


def _patch_resource(
    metadata_file: str,
    file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """should be interactive, show diff, let user select what to patch"""
    raise NotImplementedError("Parallel will be implemented soon.")


def _patch_resource_hash(
    package_name: str,
    resource_name: str,
    local_resource_path: str,
    hash_algorithm: HashTypes,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    cfg_other = config_for_instance(config[section]["other"], ckan_instance)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance
    )

    ckan = CKAN(**cfg_ckan_api)

    hash_rem = hash_remote(
        ckan_api_input=cfg_ckan_api,
        secure_interface_input=cfg_secure_interface,
        ckan_storage_path=cfg_other["ckan_storage_path"],
        package_name=package_name,
        resource_id_or_name=resource_name,
        hash_type=hash_algorithm,
    )

    if local_resource_path is not None:
        local_resource_path = pathlib.Path(local_resource_path)
        hash_func = get_hash_func(hash_algorithm)
        hash_loc = hash_func(local_resource_path)
        if hash_rem != hash_loc:
            raise ValueError(
                f"The local file '{local_resource_path.as_posix()}' and the remote file do not have the same hash!\n"
                f"local hash: '{hash_loc}'\n"
                f"remote hash: '{hash_rem}'"
            )

    resource_id = ckan.resolve_resource_id_or_name_to_id(package_name, resource_name)[
        "id"
    ]

    ckan.patch_resource_metadata(
        resource_id=resource_id,
        resource_data_to_update={"hash": hash_rem, "hashtype": hash_algorithm.value},
    )


def _patch_metadata(
    package_name: str,
    metadata_file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """probably needs its own implementation independent of ckanapi"""
    metadata_file = pathlib.Path(metadata_file)

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    metadata = read_cache(metadata_file)

    ckan = CKAN(**cfg_ckan_api)
    print(json.dumps(ckan.patch_package_metadata(package_name, metadata), indent=4))


def _patch_datacite(
    metadata_file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


# TODO implement smarter resource map, so that download will not be performed again unless flag is passed
def _publish_package(
    package_name: str,
    check_data_integrity: bool,
    create_missing_: bool,
    exclude_resources: str,
    parallel: bool,
    no_prompt: bool,
    ckan_instance_target: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
    prompt_function: Prompt.ask,
):
    if exclude_resources:
        exclude_resources = exclude_resources.split(",")
    (cwd := pathlib.Path.cwd() / TEMPORARY_DIRECTORY_NAME / package_name).mkdir(
        exist_ok=True
    )

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target
    )

    doi = cfg["lds"].get_doi(package_name)

    metadata_filtered = retrieve_and_filter_source_metadata(
        ckan_source=cfg["ckan_source"],
        package_name=package_name,
        exclude_resources=exclude_resources,
        cwd=cwd,
        package_metadata_suffix=PACKAGE_META_DATA_FILE_ENDING,
    )

    temporary_resource_names = {}
    if not parallel:
        for resource in metadata_filtered["resources"]:
            res = handle_resource_download_with_integrity_check(
                cfg_ckan_source=cfg["cfg_ckan_source"],
                resource=resource,
                check_data_integrity=check_data_integrity,
                cwd=cwd,
            )
            temporary_resource_names[res["id"]] = res["name"]

        existing_and_missing_entities = handle_missing_entities(
            ckan_source=cfg["ckan_source"],
            ckan_target=cfg["ckan_target"],
            cfg_other_target=cfg["cfg_other_target"],
            create_missing_=create_missing_,
            metadata_filtered=metadata_filtered,
        )

        # NOW ALL ENTITIES EXIST (Organization, Project, TODO Variables still need to be implemented)
        if existing_and_missing_entities["missing"]["package"]:
            create_package_raw(
                ckan_instance_target=cfg["ckan_target"],
                data=metadata_filtered,
                doi=doi,
                prepare_for_publication=True,
            )

            # TODO temporary_resource_map needs_cache also.
            for resource in metadata_filtered["resources"]:
                filepath = cwd / temporary_resource_names[resource["id"]]

                create_resource_raw_wrapped(
                    cfg_ckan_target=cfg["cfg_ckan_target"],
                    cfg_other_target=cfg["cfg_other_target"],
                    filepath=filepath,
                    resource=resource,
                    package_name=package_name,
                )
            cfg["ckan_target"].reorder_package_resources(
                package_name=metadata_filtered["name"]
            )

        elif existing_and_missing_entities["exist"]["package"]:
            patch_package_raw(
                ckan_instance_destination=cfg["ckan_target"],
                data=metadata_filtered,
                doi=doi,
                prepare_for_publication=False,
            )

            for resource in metadata_filtered["resources"]:
                filepath = cwd / temporary_resource_names[resource["id"]]

                if not cfg["ckan_target"].resource_exists(  # Create resource fresh.
                    package_name=metadata_filtered["name"],
                    resource_name=resource["name"],
                ):

                    create_resource_raw_wrapped(
                        cfg_ckan_target=cfg["cfg_ckan_target"],
                        cfg_other_target=cfg["cfg_other_target"],
                        filepath=filepath,
                        resource=resource,
                        package_name=package_name,
                    )

                    if not resource_is_link(resource):
                        hash_rem = hash_remote(
                            ckan_api_input=cfg["cfg_ckan_target"],
                            secure_interface_input=cfg["cfg_secure_interface_target"],
                            ckan_storage_path=cfg["cfg_other_target"]["ckan_storage_path"],
                            package_name=package_name,
                            resource_id_or_name=resource["name"],
                            hash_type=resource["hashtype"],
                        )

                        cfg["ckan_target"].patch_resource_metadata(
                            resource_id=cfg["ckan_target"].resolve_resource_id_or_name_to_id(
                                package_name, resource["name"]
                            )["id"],
                            resource_data_to_update={
                                "hash": hash_rem,
                                "hashtype": resource["hashtype"],
                            },
                        )

                    continue

                patch_metadata = True
                if not resource_is_link(resource):
                    resource_integrity_intact = (
                        resource_integrity_between_ckan_instances_intact(
                            ckan_api_input_1=cfg["cfg_ckan_source"],
                            ckan_api_input_2=cfg["cfg_ckan_target"],
                            package_name=metadata_filtered["name"],
                            resource_id_or_name=resource["name"],
                        )
                    )
                    if not resource_integrity_intact:
                        if not no_prompt:
                            confirmation = prompt_function(
                                f"The resource '{resource['name']}' has a different hash between "
                                f"'{ckan_instance_source}' and '{ckan_instance_target}'. "
                                f"Should it be uploaded again?",
                                choices=["no", "yes"],
                                default="no",
                            )
                            if confirmation != "yes":
                                continue

                        # resource will need re-uploading
                        upload_func = get_upload_func(
                            file_sizes=int(resource["size"]),
                            space_available_on_server_root_disk=cfg["cfg_other_target"][
                                "space_available_on_server_root_disk"
                            ],
                            parallel_upload=False,
                            factor=UPLOAD_FUNC_FACTOR,
                            is_link=resource_is_link(resource),
                        )

                        # Deleting the entire resource, and re-uploading it.
                        cfg["ckan_target"].delete_resource(
                            resource_id=cfg["ckan_target"].resolve_resource_id_or_name_to_id(
                                package_name=metadata_filtered["name"],
                                resource_id_or_name=resource["name"],
                            )
                        )

                        create_resource_raw(
                            ckan_api_input=cfg["cfg_ckan_target"],
                            secure_interface_input=cfg["cfg_secure_interface_target"],
                            ckan_storage_path=cfg["cfg_other_target"]["ckan_storage_path"],
                            package_name=metadata_filtered["name"],
                            metadata=resource,
                            file_path=filepath,
                            upload_func=upload_func,
                            progressbar=True,
                            prepare_for_publication=True,
                        )
                        patch_metadata = False

                if patch_metadata:
                    patch_resource_metadata_raw(
                        ckan_api_input=cfg["cfg_ckan_target"],
                        package_name=metadata_filtered["name"],
                        resource_name=resource["name"],
                        metadata=resource,
                        is_link=resource_is_link(resource),
                        prepare_for_publication=True,
                    )
            cfg["ckan_target"].reorder_package_resources(
                package_name=metadata_filtered["name"]
            )

        else:
            raise ValueError(
                "Oops, this should not happen, seems like the package your trying to publish "
                "is not flagged as 'missing' neither as 'existing'."
            )
    else:
        raise NotImplementedError("Parallel is not implemented yet.")

    # TODO: should this be metadata found in the ckan source instance or destination instance
    enrich_and_store_metadata(
        metadata=metadata_filtered,
        local_doi_store_instance=cfg["lds"],
        package_name=metadata_filtered["name"],
    )

    update_datacite_doi(
        datacite_api_instance=cfg["datacite"],
        local_doi_store_instance=cfg["lds"],
        package_name=package_name,
    )

    if not no_prompt:
        confirmation = prompt_function(
            "Should the doi be published? This is irreversible.",
            choices=["no", "yes"],
            default="no",
        )
        if confirmation == "yes":
            publish_datacite_doi(
                datacite_api_instance=cfg["datacite"],
                local_doi_store_instance=cfg["lds"],
                package_name=package_name,
            )
        else:
            print("Publication aborted.")

    cfg["ckan_target"].update_doi(
        package_name=metadata_filtered["name"],
        doi=doi,
        citation=get_citation_from_doi(doi),
    )


def _publish_organization(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


def _publish_project(
    project_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


def _publish_doi(
    package_name: str,
    prompt_function: Prompt.ask,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    lds = LocalDoiStore(path=config[section]["local_doi_store_path"])
    datacite = DataCiteAPI(**config[section]["datacite"])

    confirmation = prompt_function(
        "Should the doi be published? This is irreversible.",
        choices=["no", "yes"],
        default="no",
    )
    if confirmation == "yes":
        publish_datacite_doi(
            datacite_api_instance=datacite,
            local_doi_store_instance=lds,
            package_name=package_name,
        )
    else:
        print("Publication aborted.")


def _publish_controlled_vocabulary(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


def _delete_package(
    package_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    print(json.dumps(ckan.delete_package(package_id=package_name), indent=4))


def _delete_resource(
    package_name: str,
    resource_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)

    to_delete = [
        r["id"]
        for r in ckan.get_package(package_name)["resources"]
        if r["name"] == resource_name
    ]
    if not to_delete:
        sys.exit(
            f"EXIT: No resource '{resource_name}' exists in the package '{package_name}'."
        )

    for resource_id in to_delete:
        ckan.delete_resource(resource_id=resource_id)
