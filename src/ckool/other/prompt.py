from typing import Callable

import requests
from rich.prompt import Prompt

from ckool import DEFAULT_AFFILIATION


def is_yes(question: str, default: str = "no", prompt_func: Callable = Prompt.ask):
    response = prompt_func(question, choices=["yes", "no"], default=default)
    if response == "yes":
        return True
    return False


def ask_for_affiliations(
    authors: list,
    prompt_func: Callable = Prompt.ask,
    default_affiliation: str = DEFAULT_AFFILIATION,
):
    if not is_yes(
        "Do you want to provide affiliations?",
    ):
        return

    if is_yes("Provide only one affiliation for all authors?", default="yes"):
        affils = prompt_func("Affiliation", default=default_affiliation)
        return {author: affils for author in authors}

    answers = {}
    for author in authors:
        answers[author] = (
            prompt_func(
                f"Affiliation for '{author}' (keep empty to cancel)",
                default=default_affiliation,
                show_default=False,
            )
            .lstrip()
            .rstrip()
        )
    return answers


def find_publication(doi: str):
    ...


def prompt_related_publication(prompt_func: Callable = Prompt.ask):
    exists = False
    publication_info = None

    choices = {
        "relationType": [
            "Cites",
            "HasPart",
            "IsCitedBy",
            "IsNewVersionOf",
            "IsPartOf",
            "IsPreviousVersionOf",
            "IsReferencedBy",
            "IsSupplementedBy",
            "IsSupplementTo",
            "Obsoletes",
            "References",
        ],
        "resourceTypeGeneral": ["Collection", "Dataset", "Text"],
        "relatedIdentifierType": ["DOI", "ISBN", "URL"],
    }

    while not exists:
        doi = prompt_func(
            "Please provide the DOI of a related publication (keep empty to cancel)",
            default="",
            show_default=False,
        )
        if not doi:
            return

        publication_info = find_publication(doi)

        if publication_info:
            exists = True

        print("The DOI you provided could not be found. Please try again.")
    return publication_info


def ask_for_related_publications(prompt_func: Callable = Prompt.ask):
    if not is_yes(
        "Do you want to provide related publication(s)?",
    ):
        return

    pubs = []
    while publication_info := prompt_related_publication(prompt_func):
        pubs.append(publication_info)

    return pubs


def search_orcid_by_author(author):
    last_name, first_name = author.split(", ")
    base_url = "https://pub.orcid.org/v3.0/search/"
    headers = {"Accept": "application/json"}

    query = f"family-name:{last_name} AND given-names:{first_name}"
    params = {"q": query}

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        return [
            {
                "id": item["orcid-identifier"]["path"],
                "url": item["orcid-identifier"]["uri"],
            }
            for item in data["result"]
        ]
    return []


def orcid_exists(orcid: str):
    base_url = f"https://pub.orcid.org/v3.0/{orcid}"
    headers = {"Accept": "application/json"}

    response = requests.get(base_url, headers=headers)

    if response.ok:
        name = response.json()["person"]["name"]
        return f'{name["given-names"]["value"]} {name["family-name"]["value"]}'
    return False


def prompt_orcid(author: str, prompt_func: Callable = Prompt.ask):
    orcid = None
    exists = False
    found = search_orcid_by_author(author)
    proposed = False
    while not exists:
        if len(found) == 1 and not proposed:
            orcid = prompt_func(
                f"ORCiD for '{author}' found you can check it here: {found[0]['url']} (enter 'cancel' to cancel)",
                default=found[0]["id"],
            )
            proposed = True

            if sorted(orcid) == sorted("cancel"):
                orcid = None

        else:
            orcid = (
                prompt_func(
                    f"ORCiD for '{author}' (keep empty to cancel)",
                    default="",
                    show_default=False,
                )
                .lstrip()
                .rstrip()
            )
        if not orcid:  # empty input
            return

        exists = orcid_exists(orcid)
        if exists:
            print(f"... DOI registered under {exists}.")
            continue
        print("The ORCiD could not be found. Please try again.")
    return orcid


def ask_for_orcids(authors: list, prompt_func: Callable = Prompt.ask):
    if not is_yes(
        "Do you want to provide ORCiDs?",
    ):
        return

    answers = {}
    for author in authors:
        id_ = prompt_orcid(author, prompt_func)
        if id_:
            answers[author] = id_

    return answers
