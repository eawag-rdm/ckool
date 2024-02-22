import pathlib
import tomllib
from textwrap import dedent
from ckool import DEFAULT_TOML_NAME


CKOOL_TOML = dedent(
    """
    #####################################################################################
    # README
    # ------
    # Entries in the secrets section can be environment variables or
    # entries (paths) in the 'pass' password manager
    #
    #####################################################################################
    [Production]
    datacite = { user = "...", host = "...", prefix = "...", secret_password = "..."}
    
    ckan_api = [
        { instance = "...", server = "...", token = "...", secret_token = "..."},
        { instance = "...", server = "...", token = "...", secret_token = "..."},
    ]
    
    ckan_server = [
        {instance = "...", host = "...", port = ..., user = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..." },
        {instance = "...", host = "...", port = ..., user = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..."}
    ]
    other = [
        {instance = "...", ckan_storage_path = "...", space_available_on_server_root_disk=...},
        {instance = "...", ckan_storage_path = "...", space_available_on_server_root_disk=..., datamanager="..."}
    ]
    
    local_doi_store_path = "..."
    
    [Test]
    datacite = { user = "...", host = "...", prefix = "..." , secret_password = "..."}
    
    ckan_api = [
        { instance = "...", server = "...", token = "..."},
    ]
    
    ckan_server = [
        {instance = "...", host = "...", port = 5224, user = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..." },
    ]
    
    other = [
        {instance = "...", ckan_storage_path = "...", space_available_on_server_root_disk=..., datamanager="..."},
    ]
    
    local_doi_store_path = "..."

    """
).lstrip("\n")


def get_default_conf_location(default_name: str = ".ckool.toml"):
    """Function will prefer the conf in the current directory"""
    return pathlib.Path.home() / default_name


def set_config_file_as_default(config_file: pathlib.Path):
    default_destination = get_default_conf_location()
    default_destination.write_text(config_file.read_text())


def parse_config(config_file: pathlib.Path):
    with open(config_file, "rb") as f:
        return tomllib.load(f)


def generate_example_config(config_file: pathlib.Path = None):
    if config_file.exists() and config_file.is_dir():
        config_file = config_file / DEFAULT_TOML_NAME
    with open(config_file, "w+") as file:
        file.write(CKOOL_TOML)


def load_config(config_file: pathlib.Path = None):
    """This wrapper around parse config can be used to abstract away the order of the config file"""

    if config_file is not None and pathlib.Path(config_file).exists():
        return parse_config(config_file)

    home_dir, current_dir = get_default_conf_location()
    if current_dir.exists():
        return parse_config(current_dir)
    elif home_dir.exists():
        return parse_config(home_dir)

    raise FileNotFoundError(
        f"Can not found the config file. You must either make sure to have a config file named '{home_dir.name}' in your cwd, in your home directory or you must provide one via the CLI."
    )


def config_for_instance(config_subsection: list, instance_name: str):
    for section in config_subsection:
        if section.get("instance") == instance_name:
            del section["instance"]
            return section
    raise ValueError(
        f"The instance name '{instance_name}' you specified is not defined in the config file."
    )
