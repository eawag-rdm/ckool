from typing import Callable

from rich import print as rprint
from rich.prompt import Prompt

from ckool import DEFAULT_AFFILIATION
from ckool.datacite.parse_datacite_schema import SchemaParser
from ckool.interfaces.mixed_requests import doi_exists, url_exists, search_orcid_by_author, orcid_exists


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
    if not is_yes("Do you want to provide affiliations?", prompt_func=prompt_func):
        return

    if is_yes(
        "Provide only one affiliation for all authors?",
        default="yes",
        prompt_func=prompt_func,
    ):
        affils = prompt_func("Affiliation", default=default_affiliation)
        return {author: affils for author in authors}

    answers = {}
    for author in authors:
        affiliation = (
            prompt_func(
                f"Affiliation for '{author}' (enter 'skip' to skip)",
                default=default_affiliation,
                show_default=False,
            )
            .lstrip()
            .rstrip()
        )
        if not affiliation == "skip":
            answers[author] = affiliation
    return answers


def identifier_exists(identifier_info: dict):
    rel_id_typ = identifier_info["relatedIdentifier"]["att"]["relatedIdentifierType"]
    val = identifier_info["relatedIdentifier"]["val"]
    if rel_id_typ == "DOI":
        rprint(f"... checking the if the 'DOI: {val}' exists.")
        return doi_exists(val)
    elif rel_id_typ == "URL":
        rprint(f"... checking the if the 'URL: {val}' exists.")
        return url_exists(val)
    else:
        return True  # if no check implemented assuming the identifier exists


def prompt_related_identifiers(prompt_func: Callable = Prompt.ask):
    exists = False
    identifier_info = {}

    schema_latest = SchemaParser()
    while not exists:
        related_identifier_type = prompt_func(
            "What's the 'relatedIdentifierType'? (enter 'cancel' to cancel)",
            default="DOI",
            choices=schema_latest.get_schema_choices("relatedIdentifierType")
            + ["cancel"],
        )
        if related_identifier_type == "cancel":
            return
        resource_type = prompt_func(
            "What's the 'resourceType'? (enter 'cancel' to cancel)",
            default="Collection",
            choices=schema_latest.get_schema_choices("resourceType") + ["cancel"],
        )
        if resource_type == "cancel":
            return
        relation_type = prompt_func(
            "What's the 'relationType' to this package? (enter 'cancel' to cancel)",
            default="IsSupplementTo",
            choices=schema_latest.get_schema_choices("relationType") + ["cancel"],
        )
        if relation_type == "cancel":
            return
        value = prompt_func(
            "Please provide the value of the identifier. (keep empty to cancel)",
        )
        if not value:
            return

        identifier_info = {
            "relatedIdentifier": {
                "val": value,
                "att": {
                    "resourceTypeGeneral": resource_type,
                    "relatedIdentifierType": related_identifier_type,
                    "relationType": relation_type,
                },
            }
        }

        exists = identifier_exists(identifier_info)
        if not exists:
            rprint(
                f"The '{related_identifier_type}' you provided '{value}' could not be found. Please try again."
            )

    return identifier_info


def ask_for_related_identifiers(prompt_func: Callable = Prompt.ask):
    if not is_yes(
        "Do you want to provide (a) related identifier(s)?", prompt_func=prompt_func
    ):
        return

    identifiers = []
    while identifier_info := prompt_related_identifiers(prompt_func):
        identifiers.append(identifier_info)

    return identifiers


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
                    default="cancel",
                    show_default=False,
                )
                .lstrip()
                .rstrip()
            )
        if orcid == "cancel":  # empty input
            return

        exists = orcid_exists(orcid)
        if exists:
            rprint(f"... DOI registered under {exists}.")
            continue
        rprint("The ORCiD could not be found. Please try again.")
    return orcid


def ask_for_orcids(authors: list, prompt_func: Callable = Prompt.ask):
    if not is_yes("Do you want to provide ORCiDs?", prompt_func=prompt_func):
        return

    answers = {}
    for author in authors:
        id_ = prompt_orcid(author, prompt_func)
        if id_:
            answers[author] = id_

    return answers


if __name__ == "__main__":
    print(search_orcid_by_author("Dennis, Stuart"))
