import re
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from lxml import etree as ET


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
        bs = BeautifulSoup(response.text)
        links = [
            urljoin(base_url, html.find("a").get("href"))
            for html in bs.find_all("div", {"class": "lib4ridora-pdf-link"})
        ]

        if not links:
            print(f"WARNING: No DORA entry for DOI '{doi}'")
            return
        if len(links) > 1:
            print(
                f"WARNING: multiple DORA records for one DOI: '{doi}'. Only the first will be returned"
            )

        return links[0]

    # TODO
    @classmethod
    def _get_dora_record(cls, dora_id):
        url = f"https://www.dora.lib4ri.ch/eawag/islandora/object/{dora_id}/datastream/MODS"
        res = requests.get(url).text
        root = ET.fromstring(res)
        ns = {"nsdefault": re.match("{(.*)}.*", root.tag).group(1)}
        return ns, root

    # TODO
    # This relies on the current position of the
    # Data-Link in DORA-MODS. Not robust.
    @classmethod
    def _get_paperdoi_from_dora(cls, dora_id):
        ns, root = cls._get_dora_record(dora_id)
        related_items = root.findall("./nsdefault:identifier[@type='doi']", ns)
        paperdoi = related_items[0].text
        return paperdoi

    # TODO
    @classmethod
    def _doi_from_publicationlink(cls, publicationlink):
        try:
            doi = re.match(r"^https?://(dx.)?doi.org/(.*)$", publicationlink).group(2)
        except AttributeError:
            try:
                dora_id = re.match(
                    r"^https?://(www.)?interfaces.lib4ri.ch.*/(.*)$", publicationlink
                ).group(2)
                doi = cls._get_paperdoi_from_dora(dora_id)
            except Exception as e:
                print(
                    f"WARNING: publicationlink ({publicationlink}) neither recognized as DOI nor as DORA-link"
                )
                raise e
        return doi
