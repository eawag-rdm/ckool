import pathlib

import typer
from rich.prompt import Prompt

from ckool import LOGGER
from ckool.api import (
    _delete_package,
    _delete_resource,
    _download_all_metadata,
    _download_metadata,
    _download_package,
    _download_resource,
    _get_local_resource_location,
    _patch_all_resource_hashes_in_package,
    _patch_datacite,
    _patch_metadata,
    _patch_package,
    _patch_resource,
    _patch_resource_hash,
    _prepare_package,
    _publish_controlled_vocabulary,
    _publish_doi,
    _publish_organization,
    _publish_package,
    _publish_project,
    _upload_package,
    _upload_resource,
)
from ckool.other.types import CompressionTypes, HashTypes

from .other.config_parser import (
    generate_example_config,
    get_default_conf_location,
    load_config,
    set_config_file_as_default,
)

OPTIONS = {"config": {}, "verify": True, "ckan-instance-name": "None"}

app = typer.Typer()


@app.callback()
def set_logging_level(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose log messages."
    ),
    debug: bool = typer.Option(
        False, "--debug", "-d", help="Enable debug log messages"
    ),
):
    LOGGER.reload(verbose_stream=verbose, debug_stream=debug)


config_app = typer.Typer()
app.add_typer(
    config_app,
    name="config",
    help="Generate an example configuration .toml file or set a default configuration.",
)

prepare_app = typer.Typer()
app.add_typer(
    prepare_app,
    name="prepare",
    help="Get everything ready for your package upload, compress folders and collect metadata for your files.",
)

create_app = typer.Typer()
app.add_typer(
    create_app, name="upload", help="Upload an entire package or a single resources."
)

get_app = typer.Typer()
app.add_typer(
    get_app,
    name="get",
    help="Retrieve data from ckan, metadata, resources, entire packages or maybe the local file path of a resource.",
)

patch_app = typer.Typer()
app.add_typer(
    patch_app,
    name="patch",
    help="Patching hashes, metadata, resources, packages. You can also patch datacite records.",
)

delete_app = typer.Typer()
app.add_typer(
    delete_app,
    name="delete",
    help="Delete a package or ALL resources in a package that have a certain name.",
)

publish_app = typer.Typer()
app.add_typer(
    publish_app,
    name="publish",
    help="Publish an organization, project or a data package.",
)


@create_app.callback()
@get_app.callback()
@patch_app.callback()
@prepare_app.callback()
@publish_app.callback()
@delete_app.callback()
def main(
    config_file: str = typer.Option(
        get_default_conf_location().as_posix(), "-c", "--config-file"
    ),
    no_verify: bool = typer.Option(
        False, "--no-verify", help="Skip the certificate verification for web requests."
    ),
    ckan_instance_name: str = typer.Option(
        "eric",
        "-ci",
        "--ckan-instance",
        help="Which CKAN instance run API requests against. For publishing this will be the 'source' instance.",
    ),
    test: bool = typer.Option(False, "--test", help="Run commands on test instances."),
):
    config_file = pathlib.Path(config_file)
    if not config_file.exists():
        typer.echo(f"Configuration file not found: '{config_file}'.")
        raise typer.Abort()

    OPTIONS["config"] = load_config(config_file)
    OPTIONS["config"].update({"config_file_location": config_file.as_posix()})
    OPTIONS["verify"] = not no_verify
    OPTIONS["ckan-instance-name"] = ckan_instance_name
    OPTIONS["test"] = test


@config_app.command("generate_example", help="Generate example .toml file.")
def generate_example(
    target_path: str = typer.Option(
        default=None,
        help="A folder path where the example .toml file will be saved.",
    ),
):
    if target_path is None:
        generate_example_config(pathlib.Path.cwd())
    else:
        generate_example_config(pathlib.Path(target_path))


@config_app.command(
    "set_default", help="Set and existing .toml file as the default configuration."
)
def set_default(
    filepath: str = typer.Argument(
        help="Path to your .toml file, containing your desired settings. "
        "Setting a default will save this file in your home_directory, "
        "the program will then use this file automatically if not other file is provided.",
    ),
):
    set_config_file_as_default(pathlib.Path(filepath))


@prepare_app.command("package", help="Compress folders, determine file stats.")
def prepare_package(
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    include_sub_folders: bool = typer.Option(
        False,
        "--include-sub-folders",
        "-isf",
        help="By default, any folders in the package folder will be ignored. "
        "If you provide this flag they will be included in the uploading process.",
    ),
    compression_type: CompressionTypes = typer.Option(
        CompressionTypes.zip.value,
        "--compression-type",
        "-ct",
        help="Default is zip.",
        case_sensitive=False,
    ),
    include_pattern: str = typer.Option(
        None,
        "--include-pattern",
        "-ip",
        help="Include files that follow a certain regex pattern. The default 'None' will include all files.",
    ),
    exclude_pattern: str = typer.Option(
        None,
        "--exclude-pattern",
        "-ep",
        help="Exclude files that follow a certain regex pattern. The default 'None' will not exclude any files.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256.value,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
    ignore_prepared: bool = typer.Option(
        False,
        "--ignore-prepared",
        "-ip",
        help="If the resource had already been prepared, should the previous work be ignored?",
    ),
):
    return _prepare_package(
        package_folder,
        include_sub_folders,
        include_pattern,
        exclude_pattern,
        compression_type,
        hash_algorithm,
        parallel,
        ignore_prepared,
    )


# TODO: Implement worker for any parallel calls


# TODO: add check integrity after upload as an option
@create_app.command(
    "package", help="Upload resources and their metadata to an existing package."
)
def upload_package(
    package_name: str = typer.Argument(
        help="Package name in CKAN.",
    ),
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    include_sub_folders: bool = typer.Option(
        False,
        "--include-sub-folders",
        "-isf",
        help="By default, any folders in the package folder will be ignored. "
        "If you provide this flag they will be included in the uploading process.",
    ),
    compression_type: CompressionTypes = typer.Option(
        CompressionTypes.zip.value,
        "--compression-type",
        "-ct",
        help="Default is zip.",
    ),
    include_pattern: str = typer.Option(
        None,
        "--include-pattern",
        "-ip",
        help="Include files that follow a certain regex pattern. The default 'None' will include all files.",
    ),
    exclude_pattern: str = typer.Option(
        None,
        "--exclude-pattern",
        "-ep",
        help="Exclude files that follow a certain regex pattern. The default 'None' will not exclude any files.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256.value,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
    force_scp: bool = typer.Option(
        False,
        "--force-scp",
        "-fs",
        help="Force the upload via scp instead of via the API.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
    workers: int = typer.Option(
        4,
        "--workers",
        "-w",
        help="How many workers to run in parallel.",
    ),
):
    return _upload_package(
        package_name,
        package_folder,
        include_sub_folders,
        compression_type,
        include_pattern,
        exclude_pattern,
        hash_algorithm,
        force_scp,
        parallel,
        workers,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@create_app.command("resource", help="Upload a single resource.")
def upload_resource(
    package_name: str = typer.Argument(
        help="Package name in CKAN.",
    ),
    filepath: str = typer.Argument(
        help="Filepath to the resource to upload. The resource can be a file or a folder.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256.value,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
    force_scp: bool = typer.Option(
        False,
        "--force-scp",
        "-fs",
        help="Force the upload via scp instead of via the API.",
    ),
):
    return _upload_resource(
        package_name,
        filepath,
        hash_algorithm,
        force_scp,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@get_app.command(
    "package", help="Download all resources of a package and the metadata."
)
def get_package(
    package_name: str = typer.Argument(
        help="Name of the package to download.",
    ),
    destination: str = typer.Option(
        ".", "--destination", "-d", help="Where should the package be saved."
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
):
    return _download_package(
        package_name,
        destination,
        parallel,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@get_app.command("local-path", help="Returns the local filepath of a resource.")
def get_local_resource_location(
    package_name: str = typer.Argument(
        help="Name of the package to download.",
    ),
    resource_name: str = typer.Argument(
        help="Name of the resource.",
    ),
):
    return _get_local_resource_location(
        package_name=package_name,
        resource_name=resource_name,
        config=OPTIONS["config"],
        ckan_instance_name=OPTIONS["ckan-instance-name"],
        verify=OPTIONS["verify"],
        test=OPTIONS["test"],
    )


@get_app.command("resource", help="Download a single resource.")
def get_resource(
    package_name: str = typer.Argument(
        help="Name of the package containing resource.",
    ),
    resource_name: str = typer.Argument(
        help="Name of resource to download.",
    ),
    destination: str = typer.Option(
        pathlib.Path.cwd().as_posix(),
        "--destination",
        "-d",
        help="Where should the resource be saved.",
    ),
):
    return _download_resource(
        package_name,
        resource_name,
        destination,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@get_app.command("metadata", help="Only get the metadata.")
def get_metadata(
    package_name: str = typer.Argument(
        help="Name of the package, for which to get the metadata.",
    ),
    filter_fields: str = typer.Option(
        None,
        "--filter_fields",
        "-ff",
        help="Filter metadata to certain fields. Separate multiple fields by comma.",
    ),
):
    return _download_metadata(
        package_name,
        filter_fields,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@get_app.command("all_metadata", help="Get all metadata in ckan instance.")
def get_all_metadata(
    include_private: bool = typer.Option(
        False,
        "--include-private",
        "-ip",
        help="Also return private packages.",
    ),
):
    return _download_all_metadata(
        include_private,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("package", help="Update specified files and fields of package.")
def patch_package(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
    package_name: str = typer.Argument(
        help="Name of package you want to patch.",
    ),
):
    return _patch_package(
        metadata_file,
        package_name,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("resource", help="Update specified file and fields of resource.")
def patch_resource(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
    file: str = typer.Option(
        None,
        "--file",
        "-f",
        help="If you're uploading a file resource, please provide the filepath.",
    ),
):
    return _patch_resource(
        metadata_file,
        file,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command(
    "resource_hash", help="Update the 'hash' and 'hashtype' field for resource."
)
def patch_resource_hash(
    package_name: str = typer.Argument(
        help="Name of package.",
    ),
    resource_name: str = typer.Argument(
        help="Name of resource. If multiple resources with the same name exist, provide the id.",
    ),
    local_resource_path: str = typer.Option(
        None,
        "--local-resource-path",
        "-lrp",
        help="If provided a resource integrity check will be run. Without the resource will only be hashed remotely.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256.value,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
):
    return _patch_resource_hash(
        package_name,
        resource_name,
        local_resource_path,
        hash_algorithm,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command(
    "all_resource_hashes",
    help="Update the 'hash' and 'hashtype' field for all resources a package.",
)
def patch_all_resource_hashes_in_package(
    package_name: str = typer.Argument(
        help="Name of package.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256.value,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
):
    return _patch_all_resource_hashes_in_package(
        package_name,
        hash_algorithm,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("metadata", help="Update specified metadata fields of package only.")
def patch_metadata(
    package_name: str = typer.Argument(
        help="Name of package you want to patch.",
    ),
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
):
    return _patch_metadata(
        package_name,
        metadata_file,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("datacite", help="Update datacite record.")
def patch_datacite(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
):
    return _patch_datacite(
        metadata_file,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


# TODO exclude resource flags comma separated list
#  check hashes
#  restricted sources can't be published by default
@publish_app.command(
    "package",
    help="Publish a package, retrieving the data from one ckan instance, "
    "enriching the metadata and uploading it to another ckan instance.",
)
def publish_package(
    package_name: str = typer.Argument(
        help="Name of the data package you would like to publish.",
    ),
    projects_to_publish: str = typer.Option(
        None,
        "--projects-to-publish",
        "-ptp",
        help="Name or names if project(s) to publish the data package under. Separate with comma, if multiple.",
    ),
    check_data_integrity: bool = typer.Option(
        False,
        "--check-data-integrity",
        "-cdi",
        help="Check data integrity (hash) after download and upload, for each resource.",
    ),
    create_missing: bool = typer.Option(
        False,
        "--create-missing",
        "-cm",
        help="Create missing organization and projects required.",
    ),
    exclude_resources: str = typer.Option(
        None,
        "--exclude-resources",
        "-er",
        help="Resource names to exclude from the publication process. Separate resource_names fields by comma. "
        "If multiple resources in the package share the same name, resource_ids must be provided.",
    ),
    force_scp: bool = typer.Option(
        False,
        "--force-scp",
        "-fs",
        help="Force the upload via scp instead of via the API.",
    ),
    only_hash_source_if_missing: bool = typer.Option(
        True,
        "--hash-source-resources",
        "-hsr",
        help="By default the hash for each resource in the ckan source instance is only calculated if the field 'hash' "
        "or 'hashtype' are missing. If this flag is provided the all resources will be rehashed regardless.",
    ),
    re_download_resources: bool = typer.Option(
        False,
        "--re-download-resources",
        "-rdr",
        help="Resources are typically only downloaded, if they're not yet available locally. If resources have changed "
        "on the ckan source instance, you can pass this flag to re-download all resources.",
    ),
    keep_resources: bool = typer.Option(
        False,
        "--keep-resources",
        "-kr",
        help="Single resources will not be deleted after the upload.",
    ),
    no_resource_overwrite_prompt: bool = typer.Option(
        False,
        "--no-resource-overwrite-prompt",
        "-nrop",
        help="If you want to skip prompts",
    ),
    ckan_instance_target: str = typer.Option(
        None,
        "--ckan-instance-target",
        "-cit",
        help="If more than 2 instances are defined in your .ckool.toml configuration file, "
        "specify the instance to publish to.",
    ),
):
    return _publish_package(
        package_name,
        projects_to_publish,
        check_data_integrity,
        create_missing,
        exclude_resources,
        force_scp,
        only_hash_source_if_missing,
        re_download_resources,
        keep_resources,
        no_resource_overwrite_prompt,
        ckan_instance_target,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
        Prompt.ask,
    )


@publish_app.command(
    "doi",
    help="Publish a doi, moving it from the draft state to the published state.",
)
def publish_doi(
    package_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
):
    return _publish_doi(
        package_name,
        Prompt.ask,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@publish_app.command(
    "organization",
    help="Publish an organization, copying it from one ckan instance to another.",
)
def publish_organization(
    organization_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
    ckan_instance_target: str = typer.Option(
        None,
        "--ckan-instance-target",
        "-cit",
        help="If more than 2 instances are defined in your .ckool.toml configuration file, "
        "specify the instance to publish to.",
    ),
):
    return _publish_organization(
        organization_name,
        ckan_instance_target=ckan_instance_target,
        config=OPTIONS["config"],
        ckan_instance_source=OPTIONS["ckan-instance-name"],
        verify=OPTIONS["verify"],
        test=OPTIONS["test"],
    )


@publish_app.command(
    "project", help="Publish a project, copying it from one ckan instance to another."
)
def publish_project(
    project_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
    ckan_instance_target: str = typer.Option(
        None,
        "--ckan-instance-target",
        "-cit",
        help="If more than 2 instances are defined in your .ckool.toml configuration file, "
        "specify the instance to publish to.",
    ),
):
    return _publish_project(
        project_name,
        ckan_instance_target=ckan_instance_target,
        config=OPTIONS["config"],
        ckan_instance_source=OPTIONS["ckan-instance-name"],
        verify=OPTIONS["verify"],
        test=OPTIONS["test"],
    )


@publish_app.command(
    "controlled_vocabulary",
    help="Publishing the controlled vocabulary, copying it from one ckan instance to another.",
)
def publish_controlled_vocabulary(
    organization_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
):
    return _publish_controlled_vocabulary(
        organization_name,
        OPTIONS["config"],
        OPTIONS["ckan-instance-name"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@delete_app.command("package", help="Delete a package.")
def delete_package(
    package_name: str = typer.Argument(
        help="Name of the package, for which to get the metadata.",
    ),
    purge: bool = typer.Option(
        False,
        "--purge",
        "-p",
        help="Name of the package, for which to get the metadata.",
    ),
):
    confirmation = Prompt.ask(
        f"Are you sure you want to delete {'and purge ' if purge else ''}"
        f"the package '{package_name}' on '{OPTIONS['ckan-instance-name']}'?",
        choices=["no", "yes"],
        default="no",
    )
    if confirmation == "yes":
        return _delete_package(
            package_name,
            purge,
            OPTIONS["config"],
            OPTIONS["ckan-instance-name"],
            OPTIONS["verify"],
            OPTIONS["test"],
        )
    else:
        print("Deletion aborted.")


@delete_app.command(
    "resource", help="Delete all resource of a give name in a specified package."
)
def delete_resource(
    package_name: str = typer.Argument(
        help="Name of the package, that should be deleted.",
    ),
    resource_name: str = typer.Argument(
        help="Name of the resource, that should be deleted. All resources with that name will be deleted.",
    ),
):
    confirmation = Prompt.ask(
        f"Are you sure you want to delete the package '{package_name}' on '{OPTIONS['ckan-instance']}'?",
        choices=["no", "yes"],
        default="no",
    )
    if confirmation == "yes":
        return _delete_resource(
            package_name,
            resource_name,
            OPTIONS["config"],
            OPTIONS["ckan-instance-name"],
            OPTIONS["verify"],
            OPTIONS["test"],
        )
    else:
        print("Deletion aborted.")


app()
