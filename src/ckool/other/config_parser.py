import pathlib
import tomllib
from textwrap import dedent

CKOOL_TOML = dedent(
    """
    #####################################################################################
    # README
    # ------
    # Entries in the secrets section can be environment variables or
    # entries (paths) in the 'pass' password manager
    #
    #####################################################################################
    [Production.Base]
    
    datacite = { user = "user_name", url = "https://api.datacite.org", prefix = "prefix_here"}
    ckan_api = [
        { name = "instance_name_1", url = "host_url_1"},
        { name = "instance_name_2", url = "host_url_2"},
    ]
    
    ckan_server = [
        {name = "instance_name_1", host = "FQDN_1", port = 22, user = "user_name_1", ssh_key = "/path/to/key_1" },
        {name = "instance_name_2", host = "FQDN_2", port = 22, user = "user_name_2", ssh_key = "/path/to/key_2" }
    ]
    
    [Production.Secrets]
    datacite = { password = "very_secret" }
    ckan_api = [
        { name = "instance_name_1", apikey = "ckan_api_key_1"},
        { name = "instance_name_2", apikey = "ckan_api_key_2"},
    ]
    ckan_server = [
        {name = "instance_name_1", passphrase = "passphrase path in pass for ssh key here (if required)", password="if no ssh key"},
        {name = "instance_name_2", passphrase = "passphrase path in pass for ssh key here (if required)", password="if no ssh key" }
    ]
    
    [Test.Base]
    datacite = { user = "user_name_test", url = "https://api.test.datacite.org", prefix = "prefix_here_test" }

    [Test.Secrets]
    datacite = {password = "very_secret_test" }
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
