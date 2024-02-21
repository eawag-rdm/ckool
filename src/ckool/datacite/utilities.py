from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth


def list_dois_for_account_independent(
    host, username, password, client_id=None, page_size=1000
):
    if client_id is None:
        client_id = username
    response = requests.get(
        urljoin(host, "dois"),
        headers={"accept": "application/vnd.api+json"},
        params={"client-id": client_id, "page[size]": page_size},
        auth=HTTPBasicAuth(username, password),
    )
    response.raise_for_status()
    return response.json()["data"]


def list_dois_for_account(dc_instance, client_id=None, page_size=1000):
    dc_request = dc_instance._create_request()
    return dc_request.get(
        "dois",
        headers={"accept": "application/vnd.api+json"},
        params={"client-id": client_id, "page[size]": page_size},
    ).json()["data"]


def list_dois_for_prefix(dc_instance, prefix, page_size=1000):
    dc_request = dc_instance._create_request()
    return dc_request.get(
        "dois",
        headers={"accept": "application/vnd.api+json"},
        params={"prefix": prefix, "page[size]": page_size},
    ).json()["data"]


def reserve_doi(dc_instance, doi, metadata=None):
    # from datacite import DataCiteRESTClient
    # dc_instance=DataCiteRESTClient()
    return dc_instance.draft_doi(metadata=metadata, doi=doi)
