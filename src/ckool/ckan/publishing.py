from typing import Callable

import ckanapi.errors

from ckool.ckan.ckan import CKAN


def extract_names(data: dict):
    """extracts"""
    return {
        "package": data["name"],
        "organization": data["organization"]["name"],
        "resources": [r["name"] for r in data["resources"]],
        "projects": [g["name"] for g in data["groups"]],
        "variables": data["variables"],
    }


def check_existence(
    method: Callable,
    name: str,
    key: str,
    results_: dict,
):
    try:
        data_ = method(name)
        results_["exist"][key].append(name)
        return data_
    except ckanapi.errors.NotFound:
        results_["missing"][key].append(name)


def pre_publication_checks(ckan_instance_destination: CKAN, package_metadata: dict):
    names = extract_names(package_metadata)

    results = {
        "missing": {k: [] for k in names.keys()},
        "exist": {k: [] for k in names.keys()},
    }

    # Checking if package exists
    package_on_destination = check_existence(
        ckan_instance_destination.get_package, names["package"], "package", results
    )

    # Check which resources exist
    if package_on_destination:
        resources_on_destination = [
            r["name"] for r in package_on_destination["resources"]
        ]
        for r in package_metadata["resources"]:
            flag = "missing"
            if r["name"] in resources_on_destination:
                flag = "exist"
            results[flag]["resources"].append(r["name"])

    # Check if organization exists
    _ = check_existence(
        ckan_instance_destination.get_organization,
        names["organization"],
        "organization",
        results,
    )

    # Check if projects exist
    for project_name in names["projects"]:
        _ = check_existence(
            ckan_instance_destination.get_project, project_name, "projects", results
        )

    # TODO implement variable check, for that the internal
    #  formatting of variables needs to be considered, comma separated?
    # Check is variables exist ? There's no call for that. Must be a shell call command to read schema.yaml

    return results


def get_missing_organization_projects_variables(existing_and_missing_entities):
    return {
        "organization": ", ".join(
            existing_and_missing_entities["missing"]["organization"]
        ),
        "variables": ", ".join(existing_and_missing_entities["missing"]["variables"]),
        "projects": ", ".join(existing_and_missing_entities["missing"]["projects"]),
    }


def any_missing_organization_projects_variables(existing_and_missing_entities):
    return any(
        [
            v
            for v in get_missing_organization_projects_variables(
                existing_and_missing_entities
            ).values()
        ]
    )


def create_missing_organization():
    ...


def create_missing_project():
    ...


def create_missing_variables():
    print("The creation of missing variables is not implemented yet.")


def create_missing_resources():
    """This will requires it's own ckan instance, when parallel task"""
    ...


def create_missing_organization_projects_variables(
    ckan_instance_destination_args: dict,
    package_metadata: dict,
    existing_and_missing_entities: dict,
):
    create_missing_organization()
    create_missing_project()


def update_existing(existing_and_missing_entities):
    ...
