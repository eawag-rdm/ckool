import pathlib
import tomllib
from copy import deepcopy
from textwrap import dedent

from ckool import DEFAULT_TOML_NAME
from ckool.ckan.ckan import CKAN
from ckool.datacite.datacite import DataCiteAPI
from ckool.datacite.doi_store import LocalDoiStore

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
        {instance = "...", host = "...", port = ..., username = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..." },
        {instance = "...", host = "...", port = ..., username = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..."}
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
        {instance = "...", host = "...", port = 5224, username = "...", ssh_key = "...", secret_passphrase = "...", secret_password="..." },
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
    config_subsection = deepcopy(config_subsection)
    for section in config_subsection:
        if section.get("instance") == instance_name:
            del section["instance"]
            return section
    raise ValueError(
        f"The instance name '{instance_name}' you specified is not defined in the config file."
    )


def find_target_ckan_instance(
    ckan_instance_target, config, section, ckan_instance_source
):
    instances = [i["instance"] for i in config[section]["ckan_api"]]
    if ckan_instance_target is None:
        if (l := len(instances)) == 1:
            return None  # No other instance is defined
        elif l > 2:
            raise ValueError(
                f"Your configuration file '{config['config_file_location']}' "
                f"contains more than 2 resources:\n{repr(instances)}. You must specify a ckan target instance."
            )
        instances.remove(ckan_instance_source)
        return instances[0]


def parse_config_for_use(
    config: dict,
    test: bool,
    verify: bool,
    ckan_instance_source: str,
    ckan_instance_target: str = None,
):
    easy_access_config = {}

    section = "Production" if not test else "Test"

    easy_access_config["lds"] = LocalDoiStore(config[section]["local_doi_store_path"])
    easy_access_config["cfg_datacite"] = config[section]["datacite"]
    easy_access_config["datacite"] = DataCiteAPI(**easy_access_config["cfg_datacite"])

    # SOURCE INSTANCE
    easy_access_config["cfg_ckan_source"] = config_for_instance(
        config[section]["ckan_api"], ckan_instance_source
    )
    easy_access_config["cfg_ckan_source"].update({"verify_certificate": verify})
    easy_access_config["ckan_source"] = CKAN(**easy_access_config["cfg_ckan_source"])
    easy_access_config["cfg_secure_interface_source"] = config_for_instance(
        config[section]["ckan_server"], ckan_instance_source
    )
    easy_access_config["cfg_other_source"] = config_for_instance(
        config[section]["other"], ckan_instance_source
    )

    ckan_instance_target = find_target_ckan_instance(
        config=config,
        section=section,
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target,
    )

    if ckan_instance_target:
        easy_access_config["cfg_ckan_target"] = config_for_instance(
            config[section]["ckan_api"], ckan_instance_target
        )
        easy_access_config["cfg_ckan_target"].update({"verify_certificate": verify})
        easy_access_config["ckan_target"] = CKAN(
            **easy_access_config["cfg_ckan_target"]
        )
        easy_access_config["cfg_secure_interface_target"] = config_for_instance(
            config[section]["ckan_server"], ckan_instance_target
        )
        easy_access_config["cfg_other_target"] = config_for_instance(
            config[section]["other"], ckan_instance_target
        )

    return easy_access_config
