import re
import xml.etree.ElementTree as ET
from copy import deepcopy
from urllib.parse import urljoin

import requests

from ckool.interfaces.dora import Dora
from ckool.interfaces.mixed_requests import get_citation_from_doi


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


######## META DATA UPDATES


def mogrify_pkg(
    pkg, doi, maintainer_record, usage_contact_record, custom_citation_publication=None
):
    # maintainer_record = getuser(pkg['maintainer'], srcconn)
    # usage_contact_record = getuser(pkg['usage_contact'], srcconn)
    usage_contact_target = (
        usage_contact_record["fullname"] + " <" + usage_contact_record["email"] + ">"
    )

    def _normalize_author(author):
        """remove email addresses"""
        author = [re.sub(r"<.+@.+>", "", a).strip() for a in author]
        return author

    publinks = fix_publication_link(pkg.get("publicationlink"))
    paper_doi = publinks.get("paper_doi")
    publicationlink = publinks.get("publicationlink")
    publicationlink_dora = publinks.get("publicationlink_dora")
    publicationlink_url = publinks.get("publicationlink_url")
    citation_publication = custom_citation_publication or get_citation_from_doi(
        paper_doi
    )

    pkg_update = {
        "isopen": True,
        "usage_contact": None,
        "url": "https://doi.org/{}/".format(doi),
        "doi": doi,
        "author": _normalize_author(pkg.get("author")),
        "reviewed_by": (
            None if pkg.get("review_level") in [None, "none"] else "redacted"
        ),
        "notes-2": None,
        "maintainer": maintainer_record["fullname"],
        "maintainer_email": usage_contact_target,
        "internal_id": pkg.get("id"),
        "id": None,
        "owner_org": pkg["organization"]["name"],
        "spatial": (
            pkg.get("spatial").strip() if pkg.get("spatial") else pkg.get("spatial")
        ),
        "resources": [],
        "citation": get_citation_from_doi(doi),
        "paper_doi": paper_doi,
        "citation_publication": citation_publication,
        "publicationlink": publicationlink,
        "publicationlink_dora": publicationlink_dora,
        "publicationlink_url": publicationlink_url,
    }

    pkg.update(pkg_update)
    return pkg


def mogrify_project(project):
    new_proj = {}
    fields2copy = ["title", "description", "name"]
    for f in fields2copy:
        new_proj[f] = project[f]
    return new_proj


def mogrify_resource(resource):
    identical = ["name", "resource_type", "citation"]
    metadata = {}
    for key in identical:
        _key = resource.get(key, "")
        if _key:
            metadata[key] = _key
        else:
            if key == "citation":
                metadata[key] = ""

    if resource.get("restricted_level") != "public":
        raise Exception("Resource {} is restricted. Aborting")

    metadata["restricted_level"] = "public"
    metadata["allowed_users"] = ""
    if resource.get("publication") == "yes":
        metadata["publication"] = "yes"
    return metadata


def update_citation_info(
    metadata: dict, paper_doi: str = None, citation_publication: str = None
):
    """Updating citation info for package."""
    metadata = deepcopy(metadata)
    if paper_doi:
        metadata.update(
            {
                "paper_doi": paper_doi,
                "publicationlink": f"https://doi.org/{paper_doi}",
                "citation_publication": get_citation_from_doi(paper_doi),
            }
        )
    if citation_publication:
        metadata.update({"citation_publication": citation_publication})

    return metadata
