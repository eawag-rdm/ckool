# _*_ coding: utf-8 _*_

"""
This module reads a json-file which contains the metadata of an
item to be published and returns an XML representation thereof.

Currently, the application is DOI-minting and the DataCite metadata-schema v4.4
(https://schema.datacite.org/meta/kernel-4.4/metadata.xsd) is returned. (This
of course only happens if the input-metadata is DataCite compatible.)

The item's metadata in json is represented with regard to the XML nodes of
the schema:

Each XML node is represented by an object ("node-object").
Each node-object contains only one key - value pair.
The key of a node-object corresponds to an XML tag-name.
Namespaces for tag-names are expressed in Clark's notation:
http://www.jclark.com/xml/xmlns.htm.
The value of a node-object can be a (1) string, (2) an array or (3) an object.

(1) and (2) are simpliifcations, (3) Is most flexible.

(1) In case it is a string, it equals the XML node's text. The node
does not have attributes or children.

(2) In case it is an array, the array contains node-objects of the node's children.
The node does not have neither text nor attributes.

(3) In case it is an object, it might contain keys "val", "att", "children", "tail"

  + The value of "val" is a string and equals the XML node's text.
  + The value of "att" is an object and corresponds the node's attributes.
    Namespaces for attributes are given using Clark's notation:
    http://www.jclark.com/xml/xmlns.htm.
  + The value of "children" is an array that contains node-objects of the node's
    children.
  + The value of "tail" is text that is directly inserted after the element.
    This serves to enable "mixed contenet" (see https://lxml.de/tutorial.html#elements-contain-text).

"""
import pathlib

from lxml import etree as ET
from lxml.builder import ElementMaker

__THIS_FOLDER = pathlib.Path(__file__).parent.resolve()


def read_official_datacite_schema(typ):
    """
    Reads local datacite schema for specified type.
    Currently implemented:
    datacite4.1
    datacite4.4
    """
    global __THIS_FOLDER
    if typ == "datacite4.1":
        return ET.XMLSchema(
            ET.parse(__THIS_FOLDER / "schema/datacite/metadata_schema_4.1.xsd")
        )
    elif typ == "datacite4.4":
        return ET.XMLSchema(
            ET.parse(__THIS_FOLDER / "schema/datacite/metadata_schema_4.4.xsd")
        )
    else:
        raise ValueError(
            f"The schema type you provided'{typ}' is not implemented. "
            f"Types are expected to be in form of 'dataciteX.X'."
        )


def generate_attribute_map(typ):
    if typ == "datacite4.1":
        return {
            "lang": "{http://www.w3.org/XML/1998/namespace}lang",
        }
    elif typ == "datacite4.4":
        return {
            "lang": "{http://www.w3.org/XML/1998/namespace}lang",
        }
    else:
        return {}


def generate_attribute_defaults(typ):
    if typ == "datacite4.1":
        return {
            "resource": {
                "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation": "http://datacite.org/schema/kernel-4 "
                "http://schema.datacite.org/meta/kernel-4.1/metadata.xsd"
            }
        }
    elif typ == "datacite4.4":
        return {
            "resource": {
                "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation": "http://datacite.org/schema/kernel-4 "
                "http://schema.datacite.org/meta/kernel-4.4/metadata.xsd"
            }
        }
    else:
        return {}


class MetaDataToXMLConverter:
    def __init__(self, metadata: dict, typ="datacite4.4"):
        self.E = ElementMaker(nsmap={None: "http://datacite.org/schema/kernel-4"})
        self.typ = typ
        self.meta = metadata

        self.official_datacite_schema = None
        self.attribute_defaults = None
        self.attribute_map = None
        self.root = None

    def _validate(self):
        valid = self.official_datacite_schema.validate(
            ET.fromstring(ET.tostring(self.root))
        )
        if not valid:
            print(self.official_datacite_schema.error_log)

    def _build_tree(self, d=None):
        """Traverses the json-metadata and builds the corresponding lxml-tree"""
        d = d or self.meta
        print(d)
        assert len(d) == 1
        k = list(d.keys())[0]
        v = list(d.values())[0]
        default_att = self.attribute_defaults.get(k)
        if isinstance(v, str):
            # simple element
            el = self.E(k)
            el.text = v
            if default_att:
                el.attrib.update(default_att)
            return el
        if isinstance(v, list):
            # element containing sequence of child elements / no attributes
            el = self.E(k)
            for child in v:
                el.append(self._build_tree(d=child))
            if default_att:
                el.attrib.update(default_att)
            return el
        if isinstance(v, dict):
            # element with attribute(s)
            el = self.E(k)
            att = v.get("att")
            if att:
                att = {self.attribute_map.get(k) or k: v for k, v in att.items()}
                el.attrib.update(att)
            if default_att:
                el.attrib.update(default_att)
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
        self._validate()

        return ET.tostring(
            self.root, encoding="utf-8", xml_declaration=True, pretty_print=pretty_print
        ).decode("utf-8")
