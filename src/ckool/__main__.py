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
    _prepare_package,
    _publish_controlled_vocabulary,
    _publish_organization,
    _publish_package,
    _upload_package,
    _upload_resource,
)
from ckool.other.types import CompressionTypes

from .other.config_parser import (
    generate_example_config,
    get_default_conf_location,
    load_config,
    set_config_file_as_default,
)

# TODO: default should be ignore subfolders; add --include-sub-folders

# TODO: publish organizations would be useful


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
        help="Which CKAN instance run API requests against.",
    ),
    test: bool = typer.Option(False, "--test", help="Run commands on Test instances."),
):
    config_file = pathlib.Path(config_file)
    if not config_file.exists():
        typer.echo(f"Configuration file not found: '{config_file}'.")
        raise typer.Abort()

    OPTIONS["config"] = load_config(config_file)
    OPTIONS["verify"] = not no_verify
    OPTIONS["ckan-instance"] = ckan_instance
    OPTIONS["test"] = test


@config_app.command("generate_example", help="Generate example .toml file.")
def generate_example(
    filepath: Annotated[
        str,
        typer.Option(
            help="A filepath where the example .toml file will be saved.",
        ),
    ]
):
    generate_example_config(pathlib.Path(filepath))


@config_app.command(
    "set_default", help="Set and existing .toml file as the default configuration."
)
def set_default(
    filepath: Annotated[
        str,
        typer.Option(
            help="Path to your .toml file, containing your desired settings. "
            "Setting a default will save this file in your home_directory, "
            "the program will then use this file automatically if not other file is provided.",
        ),
    ]
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
        help="Available compression types are 'zip' and 'tar'.",
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
    hash_algorithm: str = typer.Option(
        "sha256",
        "--hash-algorithm",
        "-ha",
        help="Which hash algorthm to use.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
):
    package_folder = pathlib.Path(package_folder)
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
        help="[zip, tar] are available compression types.",
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
    hash_algorithm: str = typer.Option(
        "sha256",
        "--hash-algorithm",
        "-ha",
        help="Which hash algorthm to use.",
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
    hash_algorithm: str = typer.Option(
        "sha256",
        "--hash-algorithm",
        "-ha",
        help="Which hash algorthm to use.",
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
    chunk_size: int = typer.Option(
        8192,
        "--chunk-size",
        "-cs",
        help="Chunk size to use for download [bytes].",
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
        chunk_size,
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
        pathlib.Path.cwd(),
        "--destination",
        "-d",
        help="Where should the resource be saved.",
    ),
    name: str = typer.Option(
        None, "--name", "-n", help="Where should the resource be saved."
    ),
    chunk_size: int = typer.Option(
        8192,
        "--chunk-size",
        "-cs",
        help="Chunk size to use for download [bytes].",
    ),
):
    return _download_resource(
        url,
        destination,
        name,
        chunk_size,
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
        pathlib.Path.cwd(),
        "--destination",
        "-d",
        help="Where should the resources be saved.",
    ),
    chunk_size: int = typer.Option(
        8192,
        "--chunk-size",
        "-cs",
        help="Chunk size to use for download [bytes].",
    ),
):
    return _download_resources(
        url_file,
        destination,
        chunk_size,
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
def download_all_metadata():
    return _download_all_metadata(
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
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    parallel: bool = typer.Option(
        False,
        "--parallel",
        "-p",
        help="Use multiple threads/processes to handle job.",
    ),
    skip_prompt: bool = typer.Option(
        False,
        "--skip-prompt",
        "-sp",
        help="Do not ask which resources to overwrite. All resources with different local hashes will be uploaded.",
    ),
    recollect_file_stats: bool = typer.Option(
        False,
        "--recollect",
        "-rc",
        help="Recollect filestats (size, hash) for all files in package folder.",
    ),
):
    return _patch_package(
        metadata_file,
        package_folder,
        parallel,
        skip_prompt,
        recollect_file_stats,
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


@patch_app.command("metadata")
def patch_metadata(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
):
    return _patch_metadata(
        metadata_file,
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
    track_progress: bool = typer.Option(
        False,
        "--track_progress",
        "-tp",
        help="Keep a record of the progress so that, "
        "if some part of the operation fails, only outstanding operations will be resumed.",
    ),
):
    return _publish_package(
        package_name,
        check_data_integrity,
        track_progress,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
        OPTIONS["test"],
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
