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
    """Function will prefer the conf in the user's home directory"""
    return {
        "hdir": pathlib.Path.home() / default_name,
        "wdir": pathlib.Path.cwd() / default_name,
    }


def parse_config(config_file: str | pathlib.Path):
    with open(config_file, "rb") as f:
        return tomllib.load(f)


def write_default_conf(config_file: str | pathlib.Path = None):
    hdir_wdir = get_default_conf_location()

    if config_file is None:
        config_file = hdir_wdir["hdir"]
    elif config_file == "." or config_file == "./" or config_file == hdir_wdir["wdir"]:
        config_file = hdir_wdir["wdir"]

    with open(config_file, "w+") as file:
        file.write(CKOOL_TOML)


def load_config(config_file: str | pathlib.Path):
    """This wrapper around parse config can be used to abstract away the order of the config file"""
    toml = parse_config(config_file)
    return toml
