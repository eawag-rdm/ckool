import re
import xml.etree.ElementTree as ET
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup


class Dora:
    @classmethod
    def publication_link_dora_from_doi(cls, doi):
        _doi = re.sub(r"/", r"~slsh~", doi)
        dora_url = "https://www.dora.lib4ri.ch/eawag/islandora/search/"
        parameters = (
            f"mods_titleInfo_title_mt:({_doi})^5 OR"
            f" mods_abstract_ms:({_doi})^2 OR"
            f" dc.creator:({_doi})^2 OR"
            f" mods_extension_originalAuthorList_mt:({_doi})^2 OR"
            f" dc.contributor:({_doi})^1 OR"
            f" dc.type:({_doi})^1 OR"
            f" catch_all_MODS_mt:({_doi})^1?search_string={_doi}&extension=false"
        )

        dora_query = urljoin(dora_url, quote(parameters))
        response = requests.get(dora_query)
        response.raise_for_status()
        base_url = "https://www.dora.lib4ri.ch/eawag/islandora/object/"

        # Extract links from response
        bs = BeautifulSoup(response.text, features="html")

        links_of_pdfs = [  # for now not in use
            urljoin(base_url, html.find("a").get("href"))
            for html in bs.find_all("div", {"class": "lib4ridora-pdf-link"})
        ]

        links_of_detailed_records = [  # for now not in use
            urljoin(base_url, html.find("a").get("href").split("/")[-1])
            for html in bs.find_all("div", {"class": "bib-detail-record"})
        ]

        if not links_of_detailed_records:
            print(f"WARNING: No DORA entry for DOI '{doi}'")
            return
        if len(links_of_detailed_records) > 1:
            print(
                f"WARNING: multiple DORA records for one DOI: '{doi}'. Only the first will be returned."
            )

        return links_of_detailed_records[0].replace("%3A", ":")

    @classmethod
    def get_doi_from_dora_id(cls, dora_id):
        """
        dora_id: str,
            example: eawag:20376
        """
        url = f"https://www.dora.lib4ri.ch/eawag/islandora/object/{dora_id}/datastream/MODS"

        xml = requests.get(url).text
        if re.match("<!DOCTYPE html>", xml):
            raise ValueError(f"No entries can be found for the dora_id '{dora_id}'.")

        xml_root = ET.fromstring(xml)

        match = re.match("{(.*)}.*", xml_root.tag)
        namespace = match.group(1) if match else ""

        def parse_root(search_string):
            nsmap = {"mods": namespace}
            return [
                identifier.text
                for identifier in xml_root.findall(search_string, namespaces=nsmap)
                if identifier.text
            ]

        dois = parse_root('.//mods:identifier[@type="doi"]')
        dois += parse_root('.//mods:identifier[@identifierType="DOI"]')

        related_dois = parse_root(
            './/mods:relatedIdentifier[@relatedIdentifierType="DOI"]'
        )

        if not dois:
            print(f"WARNING: No DOIs in Dora for dora_id: '{dora_id}'.")
            return

        if len(dois) > 1:
            print(
                f"WARNING: multiple DOIs for the dora_id: '{dora_id}'. Only the first will be returned."
            )
        if related_dois:
            print(
                f"INFO: related DOIs found for the dora_id: '{dora_id}'. Related DOIs: '{', '.join(related_dois)}'"
            )

        return dois[0]

    @classmethod
    def doi_from_publication_link(cls, publication_link):
        matched = re.match(r"^https?://(dx.)?doi.org/(.*)$", publication_link)
        if matched:
            return matched.group(2)

        matched = re.search("eawag:[0-9]{3,6}", publication_link)
        if matched:
            return cls.get_doi_from_dora_id(matched.group())

        raise ValueError(
            f"The publication link you provided '{publication_link}' is neither "
            f"recognized as doi.org link nor as dora-link."
        )
