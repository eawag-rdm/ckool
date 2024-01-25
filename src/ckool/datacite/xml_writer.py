import pathlib
import xml.etree.ElementTree as ET

# Register the default namespace
default_ns = "http://datacite.org/schema/kernel-4"
ET.register_namespace('', default_ns)

__THIS_FOLDER = pathlib.Path(__file__).parent.resolve()


def read_official_datacite_schema(typ):
    """
    Reads local datacite schema for specified type.
    Currently implemented:
    datacite4.1
    datacite4.4
    """
    # Note: xml.etree.ElementTree does not support XML schema validation.
    # This function will need to be adapted if schema validation is required.
    # For now, it just loads the schema file.
    if typ in ["datacite4.1", "datacite4.4"]:
        with open(__THIS_FOLDER / f"schema/datacite/metadata_schema_{typ[-3:]}.xsd", "r") as file:
            return file.read()
    else:
        raise ValueError(
            f"The schema type you provided '{typ}' is not implemented. "
            f"Types are expected to be in form of 'dataciteX.X'."
        )


def generate_attribute_map(typ):
    if typ in ["datacite4.1", "datacite4.4"]:
        return {
            "lang": "{http://www.w3.org/XML/1998/namespace}lang",
        }
    else:
        return {}


def generate_attribute_defaults(typ):
    if typ in ["datacite4.1", "datacite4.4"]:
        return {
            "resource": {
                "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation": f"http://datacite.org/schema/kernel-4 "
                f"http://schema.datacite.org/meta/kernel-{typ[-3:]}/metadata.xsd"
            }
        }
    else:
        return {}


class MetaDataToXMLConverter:
    def __init__(self, metadata: dict, typ="datacite4.4"):
        self.typ = typ
        self.meta = metadata

        self.official_datacite_schema = None
        self.attribute_defaults = None
        self.attribute_map = None
        self.root = None

    @staticmethod
    def _build_element(tag, text=None, tail=None, attrib=None):
        """Helper function to create an ElementTree Element."""
        if not tag.startswith("{"):
            tag = f"{{{default_ns}}}{tag}"
        element = ET.Element(tag, attrib if attrib else {})
        if text:
            element.text = text
        if tail:
            element.tail = tail
        return element

    def _build_tree(self, d=None):
        """Traverses the json-metadata and builds the corresponding ElementTree."""
        d = d or self.meta
        assert len(d) == 1
        k = list(d.keys())[0]
        v = list(d.values())[0]
        default_att = self.attribute_defaults.get(k, {})
        if isinstance(v, str):
            el = self._build_element(k, text=v, attrib=default_att)
            return el
        if isinstance(v, list):
            el = self._build_element(k, attrib=default_att)
            for child in v:
                el.append(self._build_tree(d=child))
            return el
        if isinstance(v, dict):
            el = self._build_element(k, attrib=default_att)
            att = v.get("att")
            if att:
                att = {self.attribute_map.get(k, k): v for k, v in att.items()}
                el.attrib.update(att)
            if v.get("val"):
                el.text = v.get("val")
            if v.get("tail"):
                el.tail = v.get("tail")
            children = v.get("children", [])
            for child in children:
                el.append(self._build_tree(d=child))
            return el

    def convert_json_to_xml(self, pretty_print=True):
        self.official_datacite_schema = read_official_datacite_schema(self.typ)
        self.attribute_defaults = generate_attribute_defaults(self.typ)
        self.attribute_map = generate_attribute_map(self.typ)

        self.root = self._build_tree()

        # Note: ElementTree does not have a built-in pretty print option.
        # The 'pretty_print' parameter will not have any effect.
        return ET.tostring(self.root, encoding="utf-8", xml_declaration=True).decode("utf-8")
