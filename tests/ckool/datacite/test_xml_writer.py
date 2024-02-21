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


def test_metadata_to_xml_converter(json_test_data, data_directory):
    md_converter = MetaDataToXMLConverter(
        json_test_data["enriched_package_metadata"], typ="datacite4.1"
    )
    xml = md_converter.convert_json_to_xml().replace("\n", "")
    xml_correct = (
        (data_directory / "enriched_package_metadata.xml").read_text().replace("\n", "")
    )
    assert xml == xml_correct

    md_converter = MetaDataToXMLConverter(
        json_test_data["enriched_package_metadata"], typ="datacite4.4"
    )
    xml = md_converter.convert_json_to_xml().replace("\n", "")
    xml_correct = (
        (data_directory / "enriched_package_metadata.xml").read_text().replace("\n", "")
    )
