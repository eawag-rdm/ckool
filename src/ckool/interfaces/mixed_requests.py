import re
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests

from ckool.interfaces.dora import Dora


def get_citation_from_doi(doi, prefix=10.25678):
    if not doi:
        return None
    if re.match(f"^{prefix}", doi):
        url = f"https://api.datacite.org/dois/{doi}?style=american-geophysical-union"
        headers = {"Accept": "text/x-bibliography"}
    else:
        url = "https://doi.org/{}".format(doi)
        headers = {"Accept": "text/x-bibliography; style=american-geophysical-union"}

    r = requests.get(url, headers=headers, timeout=40)

    if not r.ok:
        r.raise_for_status()
        raise requests.exceptions.RequestException(
            "Failed to get citation for DOI {}".format(doi)
        )

    return r.text.encode(r.encoding).decode("utf-8")


def fix_publication_link(publication_link):
    # TODO: This needs refactoring
    if not publication_link:
        return {}
    elif re.search(r"lib4ri", publication_link):
        query_url = urljoin(publication_link, "datastream/MODS")
        record = requests.get(query_url)
        root = ET.fromstring(record.text)
        ids = root.findall("{http://www.loc.gov/mods/v3}identifier")
        paper_dois = [i.text for i in ids if i.attrib["type"] == "doi"]
        if paper_dois:
            paper_doi = paper_dois[0]
            publicationlink = "https://doi.org/{}".format(paper_doi)
            return {
                "publicationlink": publicationlink,
                "publicationlink_dora": publication_link,
                "paper_doi": paper_doi,
            }
        else:
            # use DORA-link
            return {
                "publicationlink": None,
                "publicationlink_dora": publication_link,
                "paper_doi": None,
            }
    elif re.search(r"doi.org", publication_link):
        paper_doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", publication_link)
        return {
            "publicationlink": publication_link,
            "publicationlink_dora": Dora.publication_link_dora_from_doi(paper_doi),
            "paper_doi": paper_doi,
        }
    else:
        return {
            "publicationlink_url": publication_link,
            "publicationlink_dora": None,
            "paper_doi": None,
        }
