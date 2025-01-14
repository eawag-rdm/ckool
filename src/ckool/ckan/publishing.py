from copy import deepcopy
from typing import Callable

import ckanapi.errors
from rich.prompt import Prompt

from ckool.ckan.ckan import CKAN
from ckool.datacite.datacite import DataCiteAPI
from ckool.datacite.doi_store import LocalDoiStore
from ckool.datacite.metadata_formatter import MetaDataFormatter, try_splitting_authors
from ckool.datacite.xml_writer import MetaDataToXMLConverter
from ckool.other.caching import update_cache
from ckool.other.metadata_tools import (
    prepare_metadata_for_publication_package,
    prepare_metadata_for_publication_project,
    prepare_metadata_for_publication_resource,
)
from ckool.other.prompt import (
    ask_for_affiliations,
    ask_for_orcids,
    ask_for_related_identifiers,
)


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


def pre_publication_checks(
    ckan_instance_destination: CKAN,
    package_metadata: dict,
    projects_to_publish: list = None,
):
    names = extract_names(package_metadata)

    if projects_to_publish is None:
        projects_to_publish = []

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
    else:
        results["missing"]["resources"] = names["resources"]

    # Check if organization exists
    _ = check_existence(
        ckan_instance_destination.get_organization,
        names["organization"],
        "organization",
        results,
    )

    # Check if projects exist
    for project_name in names["projects"]:
        if project_name in projects_to_publish:
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


def create_organization_raw(
    ckan_instance_destination: CKAN, data: dict, datamanager: str
):
    org = deepcopy(data)
    del org["id"]
    del org["created"]
    del org["package_count"]
    del org["num_followers"]
    del org["users"]
    org["datamanager"] = datamanager
    return ckan_instance_destination.create_organization(**org)


def create_project_raw(
    ckan_instance_destination: CKAN, data: dict, prepare_for_publication: bool = True
):
    proj = deepcopy(data)
    del proj["id"]
    del proj["created"]
    del proj["package_count"]
    del proj["num_followers"]
    del proj["users"]

    if prepare_for_publication:
        proj = prepare_metadata_for_publication_project(
            proj
        )  # TODO: check if this should actually filter out fields

    return ckan_instance_destination.create_project(**proj)


def create_missing_variables():
    print("The creation of missing variables is not implemented yet.")


def format_package_metadata_raw(
    ckan_instance_source: CKAN,
    ckan_instance_target: CKAN,
    data: dict,
    doi: str = None,
    custom_citation_publication: str = None,
    prepare_for_publication: bool = True,
    project_names_to_link: list = None,
):
    proj_ids = None
    pkg = deepcopy(data)
    org_name = pkg["organization"]["name"]
    org_id = ckan_instance_target.get_organization(org_name)["id"]

    if project_names_to_link:
        proj_names = [
            group["name"]
            for group in pkg["groups"]
            if group["name"] in project_names_to_link
        ]
        proj_ids = [
            {"id": ckan_instance_target.get_project(name)["id"]} for name in proj_names
        ]
    maintainer_record = ckan_instance_source.get_user(pkg["maintainer"])
    usage_contact_record = ckan_instance_source.get_user(pkg["usage_contact"])

    pkg["owner_org"] = org_id
    fields = pkg.keys()
    for name in [
        "id",
        "resources",
        "organization",
        "groups",
        "creator_user_id",
        "metadata_created",
        "metadata_modified",
        "num_resources",
    ]:
        if name in fields:
            del pkg[name]

    if proj_ids:
        pkg["groups"] = proj_ids

    if prepare_for_publication and not doi:
        raise ValueError(
            "If you want to prepare for publication you need to also provide, a 'doi'."
        )

    if prepare_for_publication:
        pkg = prepare_metadata_for_publication_package(
            pkg=pkg,
            doi=doi,
            maintainer_record=maintainer_record,
            usage_contact_record=usage_contact_record,
            custom_citation_publication=custom_citation_publication,
        )
    return pkg


def create_package_raw(
    ckan_instance_source: CKAN,
    ckan_instance_target: CKAN,
    data: dict,
    doi: str = None,
    custom_citation_publication: str = None,
    prepare_for_publication: bool = True,
    project_names_to_link: list = None,
):
    pkg = format_package_metadata_raw(
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_target,
        data=data,
        doi=doi,
        custom_citation_publication=custom_citation_publication,
        prepare_for_publication=prepare_for_publication,
        project_names_to_link=project_names_to_link,
    )
    return ckan_instance_target.create_package(**pkg)


def patch_package_raw(
    ckan_instance_source: CKAN,
    ckan_instance_destination: CKAN,
    data: dict,
    doi: str = None,
    custom_citation_publication: str = None,
    prepare_for_publication: bool = False,
    project_names_to_link: list = None,
):
    pkg = format_package_metadata_raw(
        ckan_instance_source=ckan_instance_source,
        ckan_instance_target=ckan_instance_destination,
        data=data,
        doi=doi,
        custom_citation_publication=custom_citation_publication,
        prepare_for_publication=prepare_for_publication,
        project_names_to_link=project_names_to_link,
    )

    return ckan_instance_destination.patch_package_metadata(
        package_id=ckan_instance_destination.get_package(data["name"])["id"], data=pkg
    )


def format_resource_metadata_raw(
    metadata: dict,
    is_link: bool = False,
    prepare_for_publication: bool = True,
):
    data = deepcopy(metadata)
    fields = data.keys()
    for field in [
        "id",
        "created",
        "position",
        "last_modified",
        "metadata_modified",
        "package_id",
        "cache_last_updated",
        "cache_url",
        "datastore_active",
        "mimetype",
        "mimetype_inner",
        "allowed_users",
        "revision_id",
    ]:
        if field in fields:
            del data[field]

    if not is_link:
        del data["url"]
        del data["url_type"]

    if prepare_for_publication:
        data = prepare_metadata_for_publication_resource(data)

    return data


def create_resource_raw(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    metadata: dict,
    file_path: str,
    upload_func: Callable,
    progressbar: bool = True,
    is_link: bool = False,
    prepare_for_publication: bool = True,
):
    data = format_resource_metadata_raw(
        metadata=metadata,
        is_link=is_link,
        prepare_for_publication=prepare_for_publication,
    )

    return upload_func(
        ckan_api_input=ckan_api_input,
        secure_interface_input=secure_interface_input,
        ckan_storage_path=ckan_storage_path,
        package_name=package_name,
        filepath=file_path,
        metadata=data,
        progressbar=progressbar,
    )


def patch_resource_metadata_raw(
    ckan_api_input: dict,
    package_name: str,
    resource_name: str,
    metadata: dict,
    is_link: bool = False,
    prepare_for_publication: bool = True,
):
    data = format_resource_metadata_raw(
        metadata=metadata,
        is_link=is_link,
        prepare_for_publication=prepare_for_publication,
    )

    ckan = CKAN(**ckan_api_input)
    rsc_id = ckan.resolve_resource_id_or_name_to_id(
        package_name=package_name, resource_id_or_name=resource_name
    )["id"]

    return ckan.patch_resource_metadata(
        resource_id=rsc_id, resource_data_to_update=data
    )


def collect_missing_entity(
    ckan_instance_source: CKAN, existing_and_missing_entities: dict
):
    for name in existing_and_missing_entities["missing"]["projects"]:
        yield {"entity": "project", "data": ckan_instance_source.get_project(name)}
    for name in existing_and_missing_entities["missing"]["organization"]:
        yield {
            "entity": "organization",
            "data": ckan_instance_source.get_organization(name),
        }


def create_missing_organization_projects_variables(
    ckan_instance_destination: CKAN,
    entity: str,
    data: dict,
    org_data_manager: str,
    prepare_for_publication: bool,
):
    created = []
    if entity.startswith("organization"):
        created.append(
            {
                "organization": create_organization_raw(
                    ckan_instance_destination, data, org_data_manager
                )
            }
        )
    elif entity.startswith("project"):
        created.append(
            {
                "project": create_project_raw(
                    ckan_instance_destination, data, prepare_for_publication
                )
            }
        )
    return created


def enrich_and_store_metadata(
    metadata,
    local_doi_store_instance: LocalDoiStore,
    package_name: str,
    ask_orcids: bool = True,
    ask_affiliations: bool = True,
    ask_related_identifiers: bool = True,
    prompt_function: Prompt.ask = Prompt.ask,
):
    filepath_xml = local_doi_store_instance.generate_xml_filepath(package_name)

    authors = try_splitting_authors(metadata["author"])
    if ask_orcids:
        orcids = ask_for_orcids(authors=authors, prompt_func=prompt_function)
        if orcids:
            update_cache(
                orcids, local_doi_store_instance.generate_orcids_filepath(package_name)
            )

    if ask_affiliations:
        affiliations = ask_for_affiliations(
            authors=authors, prompt_func=prompt_function
        )
        if affiliations:
            update_cache(
                affiliations,
                local_doi_store_instance.generate_affiliations_filepath(package_name),
            )

    if ask_related_identifiers:
        related_identifiers = ask_for_related_identifiers(prompt_func=prompt_function)
        if related_identifiers:
            update_cache(
                related_identifiers,
                local_doi_store_instance.generate_related_publications_filepath(
                    package_name
                ),
            )

    # enrich metadata save json
    mdf = MetaDataFormatter(
        package_metadata=metadata,
        doi=local_doi_store_instance.get_doi(package_name),
        outfile=filepath_xml.with_suffix(".json"),
        affiliations=local_doi_store_instance.get_affiliations(package_name),
        orcids=local_doi_store_instance.get_orcids(package_name),
        related_publications=local_doi_store_instance.get_related_publications(
            package_name
        ),
        resource_type="Publication Data Package",
        resource_type_general="Collection",
        version="1.0",
    )
    enriched_metadata = mdf.main()

    # convert to xml and save
    mdxmlc = MetaDataToXMLConverter(enriched_metadata, typ="datacite4.4")
    xml = mdxmlc.convert_json_to_xml()
    mdxmlc.write_xml(xml, filepath_xml)

    return {"json": filepath_xml.with_suffix(".json"), "xml": filepath_xml}


def update_datacite_doi(
    datacite_api_instance: DataCiteAPI,
    local_doi_store_instance: LocalDoiStore,
    package_name: str,
):
    return datacite_api_instance.doi_update(
        doi=local_doi_store_instance.get_doi(package_name),
        url=datacite_api_instance.generate_doi_url(package_name),
        metadata_xml_file=local_doi_store_instance.get_xml_file(package_name),
    )


def publish_datacite_doi(
    datacite_api_instance: DataCiteAPI,
    local_doi_store_instance: LocalDoiStore,
    package_name: str,
):
    return datacite_api_instance.doi_publish(
        doi=local_doi_store_instance.get_doi(package_name),
    )
