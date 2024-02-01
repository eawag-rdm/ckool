import pathlib

import typer
from typing_extensions import Annotated

from ckool.templates import (
    _download_package,
    _download_resource,
    _download_resources,
    _download_metadata,
    _download_all_metadata,
    _upload_package,
    _upload_resource,
)

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

create_app = typer.Typer()
app.add_typer(create_app, name="upload")

download_app = typer.Typer()
app.add_typer(download_app, name="download")

patch_app = typer.Typer()
app.add_typer(download_app, name="patch")


@create_app.callback()
@download_app.callback()
@patch_app.callback()
def main(
    config_file: str = typer.Option(
        get_default_conf_location().as_posix(), "-c", "--config-file"
    ),
    no_verify: bool = typer.Option(
        False, "--no-verify", help="Skip the certificate verification for web requests."
    ),
    ckan_instance: str = typer.Option(
        "eric",
        "-ci",
        "--ckan-instance",
        help="Which CKAN instance run API requests against.",
    ),
):
    config_file = pathlib.Path(config_file)
    if not config_file.exists():
        typer.echo(f"Configuration file not found: '{config_file}'.")
        raise typer.Abort()

    OPTIONS["config"] = load_config(config_file)
    OPTIONS["verify"] = not no_verify
    OPTIONS["ckan-instance"] = ckan_instance


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


@create_app.command("package")
def upload_package(
    metadata_file: str = typer.Argument(
        help="JSON file containing the metadata to create package with.",
    ),
    package_folder: str = typer.Argument(
        help="Folder that contain the package resources.",
    ),
    ckan_instance: str = typer.Option(
        "eric", "--ckan-instance", "-ci", help="The name must be defined in the config."
    ),
    compression_type: str = typer.Option(
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
    workers: int = typer.Option(
        None,
        "--workers",
        "-w",
        help=(
            "Parallel workers to use. Depending on the task these could be processes or threads. "
            "If argument not provided the maximum available amount will be used."
        ),
    ),
):
    return _upload_package(
        metadata_file,
        package_folder,
        compression_type,
        include_pattern,
        exclude_pattern,
        hash_algorithm,
        parallel,
        workers,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
    )


@create_app.command("resource")
def upload_resource(
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
    return _upload_resource(
        metadata_file,
        file,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
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
    workers: int = typer.Option(
        None,
        "--workers",
        "-w",
        help=(
            "Parallel workers to use. Depending on the task these could be processes or threads. "
            "If argument not provided the maximum available amount will be used."
        ),
    ),
):
    return _download_package(
        package_name,
        destination,
        chunk_size,
        parallel,
        workers,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
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
    )

#TODO
@download_app.command("metadata")
def download_metadata(
    url_file: str = typer.Argument(
        help="A file containing all urls that should be downloaded. Each one in a new line.",
    ),
    package_name: str = typer.Option(
        pathlib.Path.cwd(),
        "--destination",
        "-d",
        help="Where should the resources be saved.",
    ),
    filter_fields: list = typer.Option(
        8192,
        "--chunk-size",
        "-cs",
        help="Chunk size to use for download [bytes].",
    ),
):
    return _download_metadata(
        url_file,
        package_name,
        filter_fields,
        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
    )

#TODO
@download_app.command("all_metadata")
def download_metadata(

):
    return _download_metadata(

        OPTIONS["config"],
        OPTIONS["ckan-instance"],
        OPTIONS["verify"],
    )



@patch_app.command("package")
def download_package(name: str):
    print(f"Hello {name}")


@patch_app.command("resource")
def download_resource(name: str):
    print(f"Hello {name}")


@patch_app.command("metadata")
def download_metadata(name: str):
    print(f"Hello {name}")


if __name__ == "__main__":
    app()
