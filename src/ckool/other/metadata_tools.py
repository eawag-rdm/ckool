import re
from copy import deepcopy

from ckool.interfaces.mixed_requests import fix_publication_link, get_citation_from_doi


def prepare_metadata_for_publication_package(
    pkg: dict,
    doi: str,
    maintainer_record: dict,
    usage_contact_record: dict,
    custom_citation_publication: str = None,
):
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
        "spatial": (
            pkg_spatial.strip() if (pkg_spatial := pkg.get("spatial")) else pkg_spatial
        ),
        # "resources": [],
        "citation": get_citation_from_doi(doi),
        "paper_doi": paper_doi,
        "citation_publication": citation_publication,
        "publicationlink": publicationlink,
        "publicationlink_dora": publicationlink_dora,
        "publicationlink_url": publicationlink_url,
    }

    pkg.update(pkg_update)
    return pkg


def prepare_metadata_for_publication_project(project: dict):
    new_proj = {}
    fields2copy = ["title", "description", "name"]
    for f in fields2copy:
        new_proj[f] = project[f]
    return new_proj


def prepare_metadata_for_publication_resource(resource: dict):
    if resource.get("restricted_level") != "public":
        raise Exception("Resource {} is restricted. Aborting")

    if "citation" not in resource.keys():
        resource["citation"] = ""
    resource["restricted_level"] = "public"
    # resource["allowed_users"] = ""
    return resource


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
