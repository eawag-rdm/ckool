from ckool import DEFAULT_TOML_NAME
from ckool.other.config_parser import generate_example_config


def test_generate_example_config_overwrite_protection(tmp_path):
    assert generate_example_config(tmp_path) == tmp_path / DEFAULT_TOML_NAME
    assert generate_example_config(tmp_path) is None
