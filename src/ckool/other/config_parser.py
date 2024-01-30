import pathlib
import tomllib
from textwrap import dedent

BASE_CONFIG = dedent(
    """
    #####################################################################################
    # README
    # ------
    # The default section has entries
    #####################################################################################
    [Upload]
    
    """
)


def parse_config(config_file: pathlib.Path):
    with open(config_file, "rb") as f:
        return tomllib.load(f)


def write_default_conf(config_file: pathlib.Path):
    with config_file.open("w+") as f:
        f.write(BASE_CONFIG)
