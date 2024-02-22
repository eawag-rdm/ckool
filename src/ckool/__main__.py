import pathlib

import typer
from rich.prompt import Prompt
from typing_extensions import Annotated

from ckool.api import (
    _delete_package,
    _download_all_metadata,
    _download_metadata,
    _download_package,
    _download_resource,
    _download_resources,
    _patch_datacite,
    _patch_metadata,
    _patch_package,
    _patch_resource,
    _patch_resource_hash,
    _prepare_package,
    _publish_controlled_vocabulary,
    _publish_organization,
    _publish_package,
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

OPTIONS = {"config": {}, "verify": True, "ckan-instance": "None"}

app = typer.Typer()

config_app = typer.Typer()
app.add_typer(
    config_app,
    name="config",
    help="Generate an example configuration .toml file or set a default configuration.",
)

prepare_app = typer.Typer()
app.add_typer(prepare_app, name="prepare")

create_app = typer.Typer()
app.add_typer(create_app, name="upload")

download_app = typer.Typer()
app.add_typer(download_app, name="download")

patch_app = typer.Typer()
app.add_typer(download_app, name="patch")

delete_app = typer.Typer()
app.add_typer(delete_app, name="delete")

publish_app = typer.Typer()
app.add_typer(publish_app, name="publish")


@create_app.callback()
@download_app.callback()
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
    ckan_instance: str = typer.Option(
        "Eric",
        "-ci",
        "--ckan-instance",
        help="Which CKAN instance run API requests against. For publishing this will be the 'source' instance.",
    ),
    test: bool = typer.Option(False, "--test", help="Run commands on Test instances."),
):
    config_file = pathlib.Path(config_file)
    if not config_file.exists():
        typer.echo(f"Configuration file not found: '{config_file}'.")
        raise typer.Abort()

    OPTIONS["config"] = load_config(config_file)
    OPTIONS["config"].update({"config_file_location": config_file.as_posix()})
    OPTIONS["verify"] = not no_verify
    OPTIONS["ckan-instance"] = ckan_instance
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


@prepare_app.command("package")
def prepare_package(
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    include_sub_folders: bool = typer.Option(
        None,
        "--include-sub-folders",
        "-isf",
        help="By default, any folders in the package folder will be ignored. "
        "If you provide this flag they will be included in the uploading process.",
    ),
    compression_type: CompressionTypes = typer.Option(
        CompressionTypes.zip,
        "--compression-type",
        "-ct",
        help="Default is zip.",
        case_sensitive=False,
    ),
    include_pattern: str = typer.Option(
        None,
        "--include-pattern",
        "-ip",
        help="Include files that follow a certain regex pattern. The default None will include all files.",
    ),
    exclude_pattern: str = typer.Option(
        None,
        "--exclude-pattern",
        "-ep",
        help="Exclude files that follow a certain regex pattern. The default None will not exclude any files.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256,
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
):
    return _prepare_package(
        package_folder,
        include_sub_folders,
        include_pattern,
        exclude_pattern,
        compression_type,
        hash_algorithm,
        parallel,
        OPTIONS["config"],
    )


@create_app.command("package")
def upload_package(
    package_name: str = typer.Argument(
        help="Package name in CKAN.",
    ),
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    include_sub_folders: bool = typer.Option(
        None,
        "--include-sub-folders",
        "-isf",
        help="By default, any folders in the package folder will be ignored. "
        "If you provide this flag they will be included in the uploading process.",
    ),
    compression_type: CompressionTypes = typer.Option(
        "zip",
        "--compression-type",
        "-ct",
        help="Default is zip.",
    ),
    include_pattern: str = typer.Option(
        None,
        "--include-pattern",
        "-ip",
        help="Include files that follow a certain regex pattern. The default None will include all files.",
    ),
    exclude_pattern: str = typer.Option(
        None,
        "--exclude-pattern",
        "-ep",
        help="Exclude files that follow a certain regex pattern. The default None will not exclude any files.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256,
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
):
    return _upload_package(
        package_name,
        package_folder,
        include_sub_folders,
        compression_type,
        include_pattern,
        exclude_pattern,
        hash_algorithm,
        parallel,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@create_app.command("resource")
def upload_resource(
    package_name: str = typer.Argument(
        help="Package name in CKAN.",
    ),
    filepath: str = typer.Argument(
        help="Filepath to the resource to upload. The resource can be a file or a folder.",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256,
        "--hash-algorithm",
        "-ha",
        help="Default is sha256.",
    ),
):
    return _upload_resource(
        package_name,
        filepath,
        hash_algorithm,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@download_app.command("package")
def download_package(
    package_name: str = typer.Argument(
        help="Name of the package to download",
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@download_app.command("resource")
def download_resource(
    url: str = typer.Argument(
        help="URL of resource.",
    ),
    destination: str = typer.Option(
        pathlib.Path.cwd().as_posix(),
        "--destination",
        "-d",
        help="Where should the resource be saved.",
    ),
):
    return _download_resource(
        url,
        destination,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@download_app.command("resources")
def download_resources(
    url_file: str = typer.Argument(
        help="A file containing all urls that should be downloaded. Each one in a new line.",
    ),
    destination: str = typer.Option(
        pathlib.Path.cwd().as_posix(),
        "--destination",
        "-d",
        help="Where should the resources be saved.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
):
    return _download_resources(
        url_file,
        destination,
        parallel,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@download_app.command("metadata")
def download_metadata(
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@download_app.command("all_metadata")
def download_all_metadata(
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("package")
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("resource")
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("resource_hash")
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
        help="If provided a resource integrity check will be run. Without the resource will only be hashed remotely",
    ),
    hash_algorithm: HashTypes = typer.Option(
        HashTypes.sha256,
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
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("metadata")
def patch_metadata(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
    package_name: str = typer.Argument(
        help="Name of package you want to patch.",
    ),
):
    return _patch_metadata(
        metadata_file,
        package_name,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@patch_app.command("datacite")
def patch_datacite(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
):
    return _patch_datacite(
        metadata_file,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


# TODO exclude resource flags comma separated list
#  check hashes
#  restricted sources can't be published by default
@publish_app.command("package")
def publish_package(
    package_name: str = typer.Argument(
        help="Name of the data package you would like to publish.",
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
        help="Create missing Organization and Projects required.",
    ),
    exclude_resources: str = typer.Option(
        None,
        "--exclude-resources",
        "-es",
        help="Resource names to exclude from the publication process. Separate resource_names fields by comma. "
        "If multiple resources in the package share the same name, resource_ids must be provided.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
    no_prompt: bool = typer.Option(
        False,
        "--no-prompt",
        "-np",
        help="If you want to skip prompts, or run publishing in parallel use this flag.",
    ),
    ckan_instance_destination: str = typer.Option(
        None,
        "--ckan-instance-destination",
        "-cid",
        help="If more than 2 instances are defined in your .ckool.toml configuration file, "
        "specify the instance to publish to.",
    ),
):
    return _publish_package(
        package_name,
        check_data_integrity,
        create_missing,
        exclude_resources,
        parallel,
        no_prompt,
        ckan_instance_destination,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
        Prompt.ask,
    )


@publish_app.command("organization")
def publish_organization(
    organization_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
):
    return _publish_organization(
        organization_name,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@publish_app.command("controlled_vocabulary")
def publish_controlled_vocabulary(
    organization_name: str = typer.Argument(
        help="Name of the organization to publish.",
    ),
):
    return _publish_controlled_vocabulary(
        organization_name,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
    )


@delete_app.command("package")
def delete_package(
    package_name: str = typer.Argument(
        help="Name of the package, for which to get the metadata.",
    ),
):
    confirmation = Prompt.ask(
        f"Are you sure you want to delete the package '{package_name}' on '{OPTIONS['ckan-instance']}'?",
        choices=["no", "yes"],
        default="no",
    )
    if confirmation == "yes":
        return _delete_package(
            package_name,
            OPTIONS["config"],
            OPTIONS["ckan-instance"],
            OPTIONS["verify"],
            OPTIONS["test"],
        )
    else:
        print("Deletion aborted.")


app()
