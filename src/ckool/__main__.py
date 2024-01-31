import pathlib

import typer
from typing_extensions import Annotated

from ckool.templates import _create_package

from .other.config_parser import (
    generate_example_config,
    get_default_conf_location,
    load_config,
    set_config_file_as_default,
)

config = None

app = typer.Typer()

config_app = typer.Typer()
app.add_typer(
    config_app,
    name="config",
    help="Generate an example configuration .toml file or set a default configuration.",
)

create_app = typer.Typer()
app.add_typer(create_app, name="create")

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
    )
):
    config_file = pathlib.Path(config_file)
    if not config_file.exists():
        typer.echo(f"Configuration file not found: '{config_file}'.")
        raise typer.Abort()
    load_config(config_file)


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
def create_package(
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
    return _create_package(
        metadata_file,
        package_folder,
        ckan_instance,
        compression_type,
        include_pattern,
        exclude_pattern,
        hash_algorithm,
        parallel,
        workers,
    )


@create_app.command("resource")
def create_resource(
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
    print(f"Hello {name}")


@create_app.command("metadata")
def create_metadata(name: str):
    print(f"Hello {name}")


@download_app.command("package")
def download_package(name: str):
    print(f"Hello {name}")


@download_app.command("resource")
def download_resource(name: str):
    print(f"Hello {name}")


@download_app.command("metadata")
def download_metadata(name: str):
    print(f"Hello {name}")


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
