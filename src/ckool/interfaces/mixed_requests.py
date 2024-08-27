import re

import requests
from bs4 import BeautifulSoup
from rich import print as rprint

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
        # r.raise_for_status()
        # raise requests.exceptions.RequestException(
        rprint("Failed to get citation for DOI {}".format(doi))
        return None

    return r.text.encode(r.encoding).decode("utf-8")


def fix_publication_link(publication_link):
    publication_link = publication_link.lstrip(" ").rstrip(
        " "
    )  # removes of leading and trailing whitespaces!
    if not publication_link:
        return {}
    elif re.search(r"lib4ri", publication_link):
        record = requests.get(publication_link)
        bs = BeautifulSoup(record.text, features="html")

        paper_dois = [
            tag.get("content") for tag in bs.find_all("meta", {"name": "citation_doi"})
        ]
        if paper_dois:
            paper_doi = paper_dois[0]
            publicationlink = f"https://doi.org/{paper_doi}"
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


def doi_exists(doi):
    response = requests.get(f"https://doi.org/{doi}")
    return response.status_code == 200


def url_exists(url):
    """Rather url accessible"""
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def _request_orcid(author, additional_filters=""):
    last_name, first_name = author.split(", ")
    base_url = "https://pub.orcid.org/v3.0/search/"
    headers = {"Accept": "application/json"}

    query = f"family-name:{last_name} AND given-names:{first_name}"
    if additional_filters:
        query += additional_filters
    params = {"q": query}

    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return []


def _format_orcid_response(data):
    return [
        {
            "id": item["orcid-identifier"]["path"],
            "url": item["orcid-identifier"]["uri"],
        }
        for item in data["result"]
    ]


def search_orcid_by_author(author):
    data = _request_orcid(author)
    if data["num-found"] == 0:
        return []

    if data and data["num-found"] == 1:
        return _format_orcid_response(data)

    data = _request_orcid(author, additional_filters=" AND affiliation-org-name: Eawag")
    if data["num-found"] == 0:
        return []

    return _format_orcid_response(data)


def orcid_exists(orcid: str):
    base_url = f"https://pub.orcid.org/v3.0/{orcid}"
    headers = {"Accept": "application/json"}

    response = requests.get(base_url, headers=headers)

    if response.ok:
        name = response.json()["person"]["name"]
        return f'{name["given-names"]["value"]} {name["family-name"]["value"]}'
    return False
