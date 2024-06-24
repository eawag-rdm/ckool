import json
import pathlib
import shutil
import sys

from rich import print as rprint
from rich.prompt import Prompt

from ckool import (
    DOWNLOAD_CHUNK_SIZE,
    HASH_BLOCK_SIZE,
    HASH_TYPE,
    LOGGER,
    PACKAGE_META_DATA_FILE_ENDING,
    TEMPORARY_DIRECTORY_NAME,
    UPLOAD_FUNC_FACTOR,
)
from ckool.ckan.ckan import CKAN
from ckool.ckan.publishing import (
    create_organization_raw,
    create_package_raw,
    create_project_raw,
    create_resource_raw,
    enrich_and_store_metadata,
    patch_package_raw,
    patch_resource_metadata_raw,
    publish_datacite_doi,
    update_datacite_doi,
)
from ckool.datacite.datacite import DataCiteAPI
from ckool.datacite.doi_store import LocalDoiStore
from ckool.interfaces.mixed_requests import get_citation_from_doi
from ckool.other.caching import read_cache
from ckool.other.config_parser import config_for_instance, parse_config_for_use
from ckool.other.file_management import get_compression_func, iter_package
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.other.utilities import resource_is_link
from ckool.parallel_runner import (
    map_function_with_processpool,
)
from ckool.templates import (
    create_resource_raw_wrapped,
    get_upload_func,
    handle_file,
    handle_folder,
    handle_folder_file,
    handle_folder_file_upload,
    handle_missing_entities,
    handle_resource_download_with_integrity_check,
    handle_upload_all,
    handle_upload_single,
    hash_all_resources,
    hash_remote,
    package_integrity_remote_intact,
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
    include_pattern: str | None,
    exclude_pattern: str | None,
    hash_algorithm: HashTypes,
    force_scp: bool,
    parallel: bool,
    workers: int,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
    progressbar: bool = True,
):
    """
    Example calls here:

    """

    LOGGER.info("Reading config.")
    section = "Production" if not test else "Test"

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_name,
        ckan_instance_target=None,
        target_needed=False,
    )

    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if not parallel:
        something_to_upload = False
        LOGGER.info(f"Iterating package folder '{package_folder.as_posix()}'.")
        for info in iter_package(
            package_folder,
            ignore_folders=not include_sub_folders,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                LOGGER.info(f"Handling file '{file.name}'.")
                _ = handle_file(
                    file,
                    hash_func,
                    hash_algorithm,
                    tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=progressbar,
                )
                something_to_upload = True

            elif folder := info["folder"]:  # folders are archived and then hashed
                if not include_sub_folders:
                    continue
                LOGGER.info(f"Handling folder '{folder.name}'.")
                handle_folder(
                    folder,
                    hash_func,
                    compression_func,
                    hash_algorithm,
                    tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=progressbar,
                )
                something_to_upload = True
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content:\n{repr(info)}"
                )
        if something_to_upload:
            LOGGER.info("Uploading resources.")
            return handle_upload_all(
                package_name,
                package_folder,
                config,
                section,
                ckan_instance_name,
                verify,
                parallel,
                progressbar=progressbar,
                force_scp=force_scp,
            )
    else:
        res = map_function_with_processpool(
            handle_folder_file_upload,
            args=None,
            kwargs=[
                dict(
                    info=info,
                    package_name=package_name,
                    include_sub_folders=include_sub_folders,
                    compression_type=compression_type,
                    hash_algorithm=hash_algorithm,
                    config=config,
                    ckan_instance_name=ckan_instance_name,
                    verify=verify,
                    section=section,
                    progressbar=progressbar,
                    force_scp=force_scp,
                )
                for info in iter_package(
                    package_folder,
                    ignore_folders=not include_sub_folders,
                    include_pattern=include_pattern,
                    exclude_pattern=exclude_pattern,
                )
            ],
            workers=workers,
        )

        cfg["ckan_source"].reorder_package_resources(package_name)
        return res


def _upload_resource(
    package_name: str,
    filepath: str,
    hash_algorithm: HashTypes,
    force_scp: bool,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    filepath = pathlib.Path(filepath)
    hash_func = get_hash_func(hash_algorithm)

    if filepath.is_file():
        LOGGER.info(f"Handling file '{filepath.name}'.")
        cache_file = handle_file(
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
    LOGGER.info("Uploading resources.")
    _ = handle_upload_single(
        metadata_file=cache_file,
        package_name=package_name,
        config=config,
        section=section,
        ckan_instance_name=ckan_instance_name,
        verify=verify,
        progressbar=True,
        force_scp=force_scp,
    )


def _prepare_package(
    package_folder: str,
    include_sub_folders: bool,
    include_pattern: str | None,
    exclude_pattern: str | None,
    compression_type: CompressionTypes,
    hash_algorithm: HashTypes,
    parallel: bool,
    ignore_prepared: bool,
    progressbar: bool = True,
):
    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)
    if ignore_prepared and (package_folder / TEMPORARY_DIRECTORY_NAME).exists():
        LOGGER.info("Deleting previously prepared caches.")
        shutil.rmtree(package_folder / TEMPORARY_DIRECTORY_NAME)

    if not parallel:
        done = []
        for info in iter_package(
            package_folder,
            ignore_folders=not include_sub_folders,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                LOGGER.info(f"Handling file '{file.name}'.")
                done.append(
                    handle_file(
                        file,
                        hash_func,
                        hash_algorithm,
                        tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                        block_size=HASH_BLOCK_SIZE,
                        progressbar=progressbar,
                    )
                )
            elif folder := info["folder"]:  # folders are archived and then hashed
                if not include_sub_folders:
                    continue

                LOGGER.info(f"Handling folder '{folder['root_folder'].name}'.")
                done.append(
                    handle_folder(
                        folder,
                        hash_func,
                        compression_func,
                        hash_algorithm,
                        tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                        block_size=HASH_BLOCK_SIZE,
                        progressbar=progressbar,
                    )
                )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content: '{repr(info)}'"
                )
        return done
    else:
        return map_function_with_processpool(
            handle_folder_file,
            args=None,
            kwargs=[
                dict(
                    info=info,
                    include_sub_folders=include_sub_folders,
                    compression_type=compression_type,
                    hash_algorithm=hash_algorithm,
                    progressbar=progressbar,
                )
                for info in iter_package(
                    package_folder,
                    ignore_folders=not include_sub_folders,
                    include_pattern=include_pattern,
                    exclude_pattern=exclude_pattern,
                )
            ],
            workers=None,  # max amount of workers will be used
        )


def _get_local_resource_location(
    package_name: str,
    resource_name: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    storage_path = config_for_instance(config[section]["other"], ckan_instance_name)[
        "ckan_storage_path"
    ]
    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info("Calling CKAN API.")
    rprint(
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
    ckan_instance_name: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(
        f"Downloading resources from package '{package_name}' to '{destination}'."
    )
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
    package_name: str,
    resource_name: str,
    destination: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    """
    Example calls here
    """
    LOGGER.info("Reading config.")

    destination = pathlib.Path(destination)
    if destination.exists() and destination.is_dir():
        destination = destination / resource_name

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(f"Downloading resource to '{destination.as_posix()}'.")
    ckan.download_resource(
        package_name=package_name,
        resource_name=resource_name,
        destination=destination,
        chunk_size=chunk_size,
    )


def _download_metadata(
    package_name: str,
    filter_fields: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    if filter_fields is not None:
        filter_fields = filter_fields.split(",")

    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(f"Dumping metadata for '{package_name}'.")
    rprint(
        json.dumps(
            ckan.get_package(package_name=package_name, filter_fields=filter_fields),
            indent=4,
        )
    )


def _download_all_metadata(
    include_private,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(f"Dumping all metadata for instance '{ckan_instance_name}'.")
    result = ckan.get_all_packages(include_private=include_private)
    if result["count"] == 1000:
        raise Warning(
            "The maximal numbers of rows were retrieved. "
            "Please check the ckanapi documentation for the package_search function "
            "on how to implement retrieval of more rows."
        )
    rprint(json.dumps(result, indent=4))


def _patch_package(
    metadata_file: str,
    package_name: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    """
    should be interactive, show diff, let user select what to patch
    probably needs its own implementation independent of ckanapi
    """
    raise NotImplementedError("Parallel will be implemented soon.")
    # section = "Production" if not test else "Test"
    # cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    # cfg_ckan_api.update({"verify_certificate": verify})
    # ckan = CKAN(**cfg_ckan_api)
    # metadata_in_ckan = ckan.get_package(package_name)


def _patch_resource(
    metadata_file: str,
    file: str,
    config: dict,
    ckan_instance_name: str,
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
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"

    cfg_other = config_for_instance(config[section]["other"], ckan_instance_name)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance_name
    )

    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(
        f"Hashing resource '{resource_name}' in package '{package_name}' remotely."
    )
    hash_rem = hash_remote(
        ckan_api_input=cfg_ckan_api,
        secure_interface_input=cfg_secure_interface,
        ckan_storage_path=cfg_other["ckan_storage_path"],
        package_name=package_name,
        resource_id_or_name=resource_name,
        hashtype=hash_algorithm,
    )

    if local_resource_path is not None:
        LOGGER.info("Hashing local resource to compare to remote hash.")
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
    LOGGER.info("Patching resource hash on CKAN.")
    ckan.patch_resource_metadata(
        resource_id=resource_id,
        resource_data_to_update={"hash": hash_rem, "hashtype": hash_algorithm.value},
    )


def _patch_all_resource_hashes_in_package(
    package_name: str,
    hash_algorithm: HashTypes,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_name,
        ckan_instance_target=None,
        target_needed=False,
    )

    LOGGER.info(f"Hashing all resources in package '{package_name}' remotely.")
    hash_all_resources(
        package_name=package_name,
        ckan_api_input=cfg["cfg_ckan_source"],
        secure_interface_input=cfg["cfg_secure_interface_source"],
        ckan_storage_path=cfg["cfg_other_source"]["ckan_storage_path"],
        hashtype=hash_algorithm,
        only_if_hash_missing=False,
    )


def _patch_metadata(
    package_name: str,
    metadata_file: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    """probably needs its own implementation independent of ckanapi"""
    LOGGER.info("Reading config.")

    metadata_file = pathlib.Path(metadata_file)

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})

    metadata = read_cache(metadata_file)

    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(f"Patching metadata for '{package_name}'.")
    rprint(json.dumps(ckan.patch_package_metadata(package_name, metadata), indent=4))


def _patch_datacite(
    metadata_file: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


# TODO should the integrity check really both times after download and after upload.
def _publish_package(
    package_name: str,
    projects_to_publish: str,
    check_data_integrity: bool,
    create_missing_: bool,
    exclude_resources: str,
    force_scp: bool,
    only_hash_source_if_missing: bool,
    re_download_resources: bool,
    keep_resources: bool,
    no_resource_overwrite_prompt: bool,
    ckan_instance_target: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
    prompt_function: Prompt.ask = Prompt.ask,
    working_directory: str = None,
):
    LOGGER.info("Reading config.")

    if exclude_resources:
        exclude_resources = exclude_resources.split(",")
    if projects_to_publish:
        projects_to_publish = projects_to_publish.split(",")

    wd = pathlib.Path.cwd()
    if working_directory is not None:
        wd = pathlib.Path(working_directory)

    (cwd := wd / TEMPORARY_DIRECTORY_NAME / package_name).mkdir(
        exist_ok=True, parents=True
    )

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target,
    )

    LOGGER.info(f"Checking remote hash information on '{ckan_instance_source}'.")
    hash_all_resources(
        package_name=package_name,
        ckan_api_input=cfg["cfg_ckan_source"],
        secure_interface_input=cfg["cfg_secure_interface_source"],
        ckan_storage_path=cfg["cfg_other_source"]["ckan_storage_path"],
        hashtype=HASH_TYPE,
        only_if_hash_missing=only_hash_source_if_missing,
    )

    doi = cfg["lds"].get_doi(package_name)

    LOGGER.info(
        f"""Filtering metadata to exclude 'only_allowed_users'"""
        f"""{" and '" + "', '".join(exclude_resources) + "'" if exclude_resources else ''}."""
    )
    metadata_filtered = retrieve_and_filter_source_metadata(
        ckan_source=cfg["ckan_source"],
        package_name=package_name,
        exclude_resources=exclude_resources,
        cwd=cwd,
        package_metadata_suffix=PACKAGE_META_DATA_FILE_ENDING,
    )

    temporary_resource_names = {}
    for resource in metadata_filtered["resources"]:
        res = handle_resource_download_with_integrity_check(
            cfg_ckan_source=cfg["cfg_ckan_source"],
            package_name=package_name,
            resource=resource,
            check_data_integrity=check_data_integrity,
            cwd=cwd,
            re_download=re_download_resources,
        )
        temporary_resource_names[res["id"]] = res["name"]

    existing_and_missing_entities = handle_missing_entities(
        ckan_source=cfg["ckan_source"],
        ckan_target=cfg["ckan_target"],
        cfg_other_target=cfg["cfg_other_target"],
        create_missing_=create_missing_,
        metadata_filtered=metadata_filtered,
        projects_to_publish=projects_to_publish,
    )

    # NOW ALL ENTITIES EXIST (Organization, Project, TODO Variables still need to be implemented)
    if existing_and_missing_entities["missing"]["package"]:
        LOGGER.info(
            f"Creating package {existing_and_missing_entities['missing']['package'][0]}..."
        )
        create_package_raw(
            ckan_instance_source=cfg["ckan_source"],
            ckan_instance_target=cfg["ckan_target"],
            data=metadata_filtered,
            doi=doi,
            prepare_for_publication=True,
            project_names_to_link=projects_to_publish,
        )

    elif existing_and_missing_entities["exist"]["package"]:
        patch_package_raw(
            ckan_instance_source=cfg["ckan_source"],
            ckan_instance_destination=cfg["ckan_target"],
            data=metadata_filtered,
            doi=doi,
            prepare_for_publication=True,
            project_names_to_link=projects_to_publish,
        )

    else:
        raise ValueError(
            "Oops, this should not happen, seems like the package your trying to publish "
            "is not flagged as 'missing' neither as 'existing' in ckool."
        )

    for resource in metadata_filtered["resources"]:
        filepath = cwd / temporary_resource_names[resource["id"]]

        if not cfg["ckan_target"].resource_exists(  # Create resource fresh.
            package_name=metadata_filtered["name"],
            resource_name=resource["name"],
        ):
            LOGGER.info(f"Uploading resource {resource['name']}...")
            create_resource_raw_wrapped(
                cfg_ckan_target=cfg["cfg_ckan_target"],
                cfg_other_target=cfg["cfg_other_target"],
                cfg_secure_interface_destination=cfg["cfg_secure_interface_target"],
                filepath=filepath,
                resource=resource,
                package_name=package_name,
                force_scp=force_scp,
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
                if not no_resource_overwrite_prompt:
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
                    file_sizes=[int(resource["size"])],
                    space_available_on_server_root_disk=cfg["cfg_other_target"][
                        "space_available_on_server_root_disk"
                    ],
                    parallel_upload=False,
                    factor=UPLOAD_FUNC_FACTOR,
                    is_link=resource_is_link(resource),
                    force_scp=force_scp,
                )

                # Deleting the entire resource, and re-uploading it.
                cfg["ckan_target"].delete_resource(
                    resource_id=cfg["ckan_target"].resolve_resource_id_or_name_to_id(
                        package_name=metadata_filtered["name"],
                        resource_id_or_name=resource["name"],
                    )["id"]
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
            LOGGER.info(f"Patching resource metadata {resource['name']}...")
            patch_resource_metadata_raw(
                ckan_api_input=cfg["cfg_ckan_target"],
                package_name=metadata_filtered["name"],
                resource_name=resource["name"],
                metadata=resource,
                is_link=resource_is_link(resource),
                prepare_for_publication=True,
            )

        # delete_local_resource after upload
        if not keep_resources:
            filepath.unlink()

    if check_data_integrity:
        package_integrity_remote_intact(
            ckan_api_input=cfg["cfg_ckan_target"],
            secure_interface_input=cfg["cfg_secure_interface_target"],
            ckan_storage_path=cfg["cfg_other_target"]["ckan_storage_path"],
            package_name=package_name,
        )

    cfg["ckan_target"].reorder_package_resources(package_name=metadata_filtered["name"])

    enrich_and_store_metadata(
        metadata=metadata_filtered,
        local_doi_store_instance=cfg["lds"],
        package_name=metadata_filtered["name"],
        ask_orcids=True,
        ask_affiliations=True,
        ask_related_identifiers=True,
        prompt_function=prompt_function,
    )

    update_datacite_doi(
        datacite_api_instance=cfg["datacite"],
        local_doi_store_instance=cfg["lds"],
        package_name=package_name,
    )

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
        LOGGER.info("Publication aborted.")

    cfg["ckan_target"].update_doi(
        package_name=metadata_filtered["name"],
        doi=doi,
        citation=get_citation_from_doi(doi),
    )


def _publish_organization(
    organization_name: str,
    ckan_instance_target: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target,
    )
    LOGGER.info(f"Retrieving data for organization '{organization_name}'.")
    organization_data = cfg["ckan_source"].get_organization(organization_name)
    LOGGER.info("... publishing.")
    created = create_organization_raw(
        cfg["ckan_target"],
        organization_data,
        datamanager=cfg["cfg_other_target"]["datamanager"],
    )
    return created


def _publish_project(
    project_name: str,
    ckan_instance_target: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    cfg = parse_config_for_use(
        config=config,
        test=test,
        verify=verify,
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target,
    )
    LOGGER.info(f"Retrieving data for project '{project_name}'.")
    project_data = cfg["ckan_source"].get_project(project_name)

    LOGGER.info("... publishing.")
    created = create_project_raw(
        cfg["ckan_target"], project_data, prepare_for_publication=False
    )
    return created


def _publish_doi(
    package_name: str,
    prompt_function: Prompt.ask,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

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
        LOGGER.info("Publication aborted.")


def _publish_controlled_vocabulary(
    organization_name: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("This feature is not implemented yet.")


def _delete_package(
    package_name: str,
    purge: bool,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    LOGGER.info(f"Deleting '{package_name}'.")
    ckan.delete_package(package_id=package_name)
    if purge:
        LOGGER.info(f"Purging '{package_name}'.")
        ckan.purge_package(package_id=package_name)


def _delete_resource(
    package_name: str,
    resource_name: str,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    test: bool,
):
    LOGGER.info("Reading config.")

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
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
        LOGGER.info(f"Deleting resource '{resource_id}'.")
        ckan.delete_resource(resource_id=resource_id)
