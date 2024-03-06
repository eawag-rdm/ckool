import os
import pathlib
import xml.etree.ElementTree as ET
from pathlib import Path

SCHEMAS = Path(__file__).parent / "schema" / "datacite"
SCHEMA_FILES = sorted(list(SCHEMAS.iterdir()))
SCHEMA_LATEST = SCHEMA_FILES[-1]


class SchemaParser:
    def __init__(self, path: pathlib.Path = SCHEMAS / SCHEMA_LATEST):
        self.path = path
        self.namespaces = {"xs": "http://www.w3.org/2001/XMLSchema"}
        self.data = self._parse_xsd_with_included(path, self.namespaces)

    @staticmethod
    def _parse_xsd_with_included(xsd_path, namespaces):
        tree = ET.parse(xsd_path)
        root = tree.getroot()

        for include in root.findall("xs:include", namespaces):
            schema_location = include.get("schemaLocation")
            included_xsd_path = os.path.join(os.path.dirname(xsd_path), schema_location)

            included_tree = ET.parse(included_xsd_path)
            included_root = included_tree.getroot()

            for child in included_root:
                root.append(child)
        return root

    def get_schema_choices(self, name: str = "resourceType"):
        resource_type_element = self.data.find(
            f".//xs:simpleType[@name='{name}'][@id='{name}']", self.namespaces
        )
        restriction_element = resource_type_element.find(
            "xs:restriction", self.namespaces
        )

        return [
            enum.get("value")
            for enum in restriction_element.findall("xs:enumeration", self.namespaces)
        ]
