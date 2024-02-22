import json
import pathlib

from rich.prompt import Prompt

from ckool import (
    DOWNLOAD_CHUNK_SIZE,
    HASH_BLOCK_SIZE,
    PACKAGE_META_DATA_FILE_ENDING,
    TEMPORARY_DIRECTORY_NAME,
    UPLOAD_FUNC_FACTOR,
)
from ckool.ckan.ckan import CKAN, filter_resources, get_resource_key
from ckool.ckan.publishing import (
    any_missing_organization_projects_variables,
    collect_missing_entity,
    create_missing_organization_projects_variables,
    create_package_raw,
    create_resource_raw,
    enrich_and_store_metadata,
    get_missing_organization_projects_variables,
    patch_package_raw,
    patch_resource_metadata_raw,
    pre_publication_checks,
    publish_datacite_doi,
    update_datacite_doi,
)
from ckool.datacite.datacite import DataCiteAPI
from ckool.datacite.doi_store import LocalDoiStore
from ckool.interfaces.mixed_requests import get_citation_from_doi
from ckool.other.caching import read_cache, update_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import get_compression_func, iter_package
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.other.utilities import resource_is_link
from ckool.parallel_runner import map_function_with_threadpool
from ckool.templates import (
    get_upload_func,
    handle_file,
    handle_folder,
    handle_upload,
    hash_remote,
    resource_integrity_between_ckan_instances_intact,
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
    config: dict,
):
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
            elif include_sub_folders and (
                folder := info["folder"]
            ):  # folders are archived and then hashed
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
        if hash_func != hash_loc:
            raise ValueError(
                f"The local file '{local_resource_path.as_posix()}' and the remote file do not have the same hash!\n"
                f"local hash: '{hash_loc}'\n"
                f"remote hash: '{hash_rem}'"
            )

    data = ckan.get_package(package_name)["resources"]
    key = get_resource_key(data, package_name, resource_name)
    if key == "id":
        resource_id = resource_name
    else:
        resource_id = [d["id"] for d in data if d[key] == resource_name][0]

    ckan.patch_resource_metadata(
        resource_id=resource_id,
        resource_data_to_update={"hash": hash_rem, "hashtype": hash_algorithm.value},
    )


def _patch_metadata(
    metadata_file: str,
    package_name: str,
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
    print(locals())
    return


def _publish_package(
    package_name: str,
    check_data_integrity: bool,
    create_missing_: bool,
    exclude_resources: str,
    parallel: bool,
    no_prompt: bool,
    ckan_instance_destination: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
    prompt_function: Prompt.ask,
):
    exclude_resources = exclude_resources.split(",")
    (cwd := pathlib.Path.cwd() / TEMPORARY_DIRECTORY_NAME / package_name).mkdir(
        exist_ok=True
    )

    section = "Production" if not test else "Test"
    local_doi_store = config[section]["local_doi_store_path"]
    instances = [i["instance"] for i in config[section]["ckan_api"]]

    if ckan_instance_destination is None:
        if len(instances) > 2 or len(instances) == 1:
            raise ValueError(
                f"Your configuration file '{config['config_file_location']}' "
                f"contains more than 2 or less that 1 resources:\n{repr(instances)}."
            )
        instances.remove(ckan_instance_source)
        ckan_instance_destination = instances[0]

    lds = LocalDoiStore(local_doi_store)
    doi = lds.get_doi(package_name)

    cfg_datacite = config[section]["datacite"]
    datacite = DataCiteAPI(**cfg_datacite)

    cfg_ckan_source = config_for_instance(
        config[section]["ckan_api"], ckan_instance_source
    )
    cfg_ckan_source.update({"verify_certificate": verify})

    cfg_ckan_destination = config_for_instance(
        config[section]["ckan_api"], ckan_instance_destination
    )
    cfg_ckan_destination.update({"verify_certificate": verify})

    cfg_secure_interface_destination = config_for_instance(
        config[section]["ckan_server"], ckan_instance_destination
    )

    cfg_other_destination = config_for_instance(
        config[section]["other"], ckan_instance_destination
    )

    ckan_source = CKAN(**cfg_ckan_source)
    ckan_destination = CKAN(**cfg_ckan_destination)

    metadata = ckan_source.get_package(package_name=package_name)

    metadata_filtered = filter_resources(
        metadata,
        resources_to_exclude=exclude_resources,
        always_to_exclude_restriction_levels=["only_allowed_users"],
    )
    # TODO: Must add check for .json.meta file in tmp dir, if it exists, the upload procedure is very different.
    package_metadata_file = (cwd / package_name).with_suffix(
        PACKAGE_META_DATA_FILE_ENDING
    )
    update_cache(metadata_filtered, cache_file=package_metadata_file)

    temporary_resource_names = {}
    if not parallel:
        metadata_filtered = read_cache(package_metadata_file)

        for resource in metadata_filtered["resources"]:
            url = resource["url"]
            id_ = resource["id"]
            temporary_resource_names[id_] = f"{id_}-{pathlib.Path(url).name}"
            downloaded_file = ckan_source.download_resource(
                url=url, destination=(cwd / temporary_resource_names[id_])
            )
            if check_data_integrity:
                if not resource["hash"]:
                    raise ValueError(f"No hash value for resource '{url}' on CKAN!")

                hash_func = get_hash_func(resource["hashtype"])
                hash_local = hash_func(downloaded_file)
                if hash_local != resource["hash"]:
                    raise ValueError(
                        f"Something went wrong. The hash value '{hash_local}' ('{resource['hashtype']}') of the "
                        f"downloaded resource '{temporary_resource_names}' does not match the one "
                        f"on CKAN '{resource['hash']}'."
                    )

        # run checks
        existing_and_missing_entities = pre_publication_checks(
            ckan_instance_destination=ckan_destination,
            package_metadata=metadata_filtered,
        )

        if create_missing_:
            if any_missing_organization_projects_variables(
                existing_and_missing_entities
            ):
                for to_create in collect_missing_entity(
                    ckan_source, existing_and_missing_entities
                ):
                    create_missing_organization_projects_variables(
                        ckan_destination,
                        **to_create,
                        org_data_manager=cfg_other_destination["datamanager"],
                        prepare_for_publication=True,
                    )

        elif any_missing_organization_projects_variables(existing_and_missing_entities):
            missing = get_missing_organization_projects_variables(
                existing_and_missing_entities
            )
            raise ValueError(
                f"Publication can not continue. These entities are missing: {repr(missing)}"
            )
        # NOW ALL ENTITIES EXIST
        if existing_and_missing_entities["missing"]["package"]:
            created = create_package_raw(
                ckan_instance_destination=ckan_destination,
                data=metadata_filtered,
                doi=doi,
                prepare_for_publication=True,
            )

            # TODO temporary_resource_map needs_cache also.
            for resource in metadata_filtered["resources"]:
                filepath = cwd / temporary_resource_names[resource["id"]]

                upload_func = get_upload_func(
                    file_sizes=int(resource["size"]),
                    space_available_on_server_root_disk=cfg_other_destination[
                        "space_available_on_server_root_disk"
                    ],
                    parallel_upload=False,
                    factor=UPLOAD_FUNC_FACTOR,
                    is_link=resource_is_link(resource),
                )

                create_resource_raw(
                    ckan_api_input=cfg_ckan_destination,
                    secure_interface_input=cfg_secure_interface_destination,
                    ckan_storage_path=cfg_other_destination["ckan_storage_path"],
                    package_name=metadata_filtered["name"],
                    metadata=resource,
                    file_path=filepath,
                    upload_func=upload_func,
                    progressbar=True,
                    prepare_for_publication=True,
                )
            ckan_destination.reorder_package_resources(
                package_name=metadata_filtered["name"]
            )

        elif existing_and_missing_entities["exist"]["package"]:
            patch_package_raw(
                ckan_instance_destination=ckan_destination,
                data=metadata_filtered,
                doi=doi,
                prepare_for_publication=False,
            )

            for resource in metadata_filtered["resources"]:
                filepath = cwd / temporary_resource_names[resource["id"]]

                patch_metadata = True
                if not resource_is_link(resource):
                    resource_integrity_intact = (
                        resource_integrity_between_ckan_instances_intact(
                            ckan_api_input_1=cfg_ckan_source,
                            ckan_api_input_2=cfg_ckan_destination,
                            package_name=metadata_filtered["name"],
                            resource_id_or_name=resource["name"],
                        )
                    )
                    if not resource_integrity_intact:
                        if not no_prompt:
                            confirmation = prompt_function(
                                f"The resource '{resource['name']}' has a different hash between "
                                f"'{ckan_instance_source}' and '{ckan_instance_destination}'. "
                                f"Should it be uploaded again?",
                                choices=["no", "yes"],
                                default="no",
                            )
                            if confirmation != "yes":
                                continue

                        # resource will need re-uploading
                        upload_func = get_upload_func(
                            file_sizes=int(resource["size"]),
                            space_available_on_server_root_disk=cfg_other_destination[
                                "space_available_on_server_root_disk"
                            ],
                            parallel_upload=False,
                            factor=UPLOAD_FUNC_FACTOR,
                            is_link=resource_is_link(resource),
                        )

                        # Deleting the entire resource, and re-uploading it.
                        ckan_destination.delete_resource(
                            resource_id=ckan_destination.resolve_resource_id_or_name_to_id(
                                package_name=metadata_filtered["name"],
                                resource_id_or_name=resource["name"],
                            )
                        )

                        create_resource_raw(
                            ckan_api_input=cfg_ckan_destination,
                            secure_interface_input=cfg_secure_interface_destination,
                            ckan_storage_path=cfg_other_destination[
                                "ckan_storage_path"
                            ],
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
                        ckan_api_input=cfg_ckan_destination,
                        package_name=metadata_filtered["name"],
                        resource_name=resource["name"],
                        metadata=resource,
                        is_link=resource_is_link(resource),
                        prepare_for_publication=True,
                    )
            ckan_destination.reorder_package_resources(
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
        local_doi_store_instance=lds,
        package_name=metadata_filtered["name"],
    )

    update_datacite_doi(
        datacite_api_instance=datacite,
        local_doi_store_instance=lds,
        package_name=metadata_filtered["name"],
    )

    if not no_prompt:
        confirmation = prompt_function(
            "Should the doi be published? This is irreversible.",
            choices=["no", "yes"],
            default="no",
        )
        if confirmation == "yes":
            publish_datacite_doi(
                datacite_api_instance=datacite,
                local_doi_store_instance=lds,
                package_name=metadata_filtered["name"],
            )
        else:
            print("Publication aborted.")

    ckan_destination.update_doi(
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
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package
    print(locals())
    pass


def _publish_project(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package
    print(locals())
    pass


def _publish_controlled_vocabulary(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())
    pass


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
