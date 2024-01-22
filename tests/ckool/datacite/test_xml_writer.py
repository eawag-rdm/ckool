import pytest

from ckool.datacite.xml_writer import (
    MetaDataToXMLConverter,
    generate_attribute_defaults,
    generate_attribute_map,
    read_official_datacite_schema,
)


def test_read_official_datacite_schema():
    read_official_datacite_schema("datacite4.1")
    read_official_datacite_schema("datacite4.4")
    with pytest.raises(ValueError):
        read_official_datacite_schema("abc")


def test_generate_attribute_map():
    generate_attribute_map("datacite4.1")
    generate_attribute_map("datacite4.4")
    assert generate_attribute_map("abc") == {}


def test_generate_attribute_defaults():
    generate_attribute_defaults("datacite4.1")
    generate_attribute_defaults("datacite4.4")
    assert generate_attribute_defaults("abc") == {}


def test_metadata_to_xml_converter(json_test_data):
    md_converter = MetaDataToXMLConverter(json_test_data["package_metadata"])
    md_converter.convert_json_to_xml()
    # TODO: get test files for published data package
