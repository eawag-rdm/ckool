import re
from base64 import b64encode
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from ckool.other.utilities import get_secret

from .doi_generator import generate_doi, revert_doi


def requests_raise_add(response, status_code, message):
    """Add useful information to status code raise of requests lib"""
    try:
        response.raise_for_status()
    except Exception as e:
        if response.status_code == status_code:
            raise DataCiteException(message) from e
        raise e


class DataCiteException(Exception):
    pass


class DataCiteAPI:
    def __init__(
        self, host, prefix, user, password=None, secret_password=None, offset=0
    ):
        if secret_password:
            password = get_secret(secret_password)
        self.auth = HTTPBasicAuth(username=user, password=password)
        self.username = user
        self.host = host
        self.prefix = prefix
        self.offset = offset

    @staticmethod
    def generate_doi_url(package_name: str):
        return f"https://opendata.eawag.ch/dataset/{package_name}"

    @staticmethod
    def _generate_unused_dois(dois, number_to_generate, prefix, offset):
        i = 0
        generated = 0
        while generated < number_to_generate:
            _doi = generate_doi(prefix, i, offset)
            if _doi not in dois:
                yield _doi
                generated += 1
            i += 1

    def doi_generate_n_strings_unused(self, n=1, offset=None):
        if offset is None:
            offset = self.offset
        dois = self.doi_list_fast()
        return [
            doi for doi in self._generate_unused_dois(dois, n, self.prefix, offset)
        ]

    def doi_list_via_client(self, client_id=None, page_size=1000, page_number=1):
        if client_id is None:
            client_id = self.username
        response = requests.get(
            url=urljoin(self.host, "dois"),
            headers={"accept": "application/vnd.api+json"},
            params={
                "client-id": client_id,
                "page[size]": page_size,
                "page[number]": page_number,
            },
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json()["data"]

    def doi_list_fast(
        self, client_id=None, page_size=1000, page_number=1
    ):
        if client_id is None:
            client_id = self.username
        response = requests.get(
            url=urljoin(self.host, "dois"),
            headers={"accept": "application/vnd.api+json"},
            params={
                "client-id": client_id,
                "fields[dois]": "doi",
                "page[size]": page_size,
                "page[number]": page_number,
            },
            auth=self.auth,
            timeout=5.0,
        )
        response.raise_for_status()
        return [d["id"] for d in response.json()["data"]]


    def _filter(self, record):
        # print("\tFiltering {}".format(record["doi"]))
        authors = record.get("creators")
        if authors:
            authors = authors[0 : min(3, len(record["creators"]))]
        else:
            authors = []
        authors = [a["name"] for a in authors]
        newrecord = {
            "doi": record.get("doi"),
            "title": (
                record.get("titles")[0]["title"] if record.get("titles") else None
            ),
            "threeauthors": authors,
            "state": record.get("state"),
            "url": record.get("url"),
            "version": record.get("version"),
        }
        return newrecord

    def doi_reserve(self, doi):
        response = requests.post(
            url=urljoin(self.host, "dois"),
            headers={"accept": "application/vnd.api+json"},
            json={"data": {"type": "dois", "attributes": {"doi": doi}}},
            auth=self.auth,
        )

        requests_raise_add(
            response, 422, f"The DOI '{doi}' you are trying to reserve already exists!"
        )

        return response

    # This updates a DOI that already exists (reserved)
    def doi_update(self, doi, url, metadata_xml_file, return_response=False):
        with open(metadata_xml_file, "rb") as f:
            xml = f.read()
        xml64 = b64encode(xml).decode()

        response = requests.put(
            url=urljoin(self.host, f"dois/{doi}"),
            headers={"accept": "application/vnd.api+json"},
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {"doi": doi, "url": url, "xml": xml64},
                }
            },
            auth=self.auth,
        )
        response.raise_for_status()
        if return_response:
            return response.json()["data"]
        return response.ok

    def doi_publish(self, doi, return_response=False):
        response = requests.put(
            url=urljoin(self.host, f"dois/{doi}"),
            headers={"accept": "application/vnd.api+json"},
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {"doi": doi, "event": "publish"},
                }
            },
            auth=self.auth,
        )
        response.raise_for_status()

        if return_response:
            return response.json()["data"]
        return response.ok

    def doi_retrieve(self, doi):
        response = requests.get(
            url=urljoin(self.host, f"dois/{doi}"),
            headers={"accept": "application/vnd.api+json"},
            auth=self.auth,
        )

        requests_raise_add(
            response, 404, f"The DOI '{doi}' you are trying to read does not exists."
        )

        return response.json()["data"]

    def doi_delete(self, doi, return_response=False):
        response = requests.delete(
            url=urljoin(self.host, f"dois/{doi}"),
            headers={"accept": "application/vnd.api+json"},
            auth=self.auth,
        )

        requests_raise_add(
            response, 404, f"The DOI '{doi}' you are trying to delete does not exists."
        )

        if return_response:
            return response.json()["data"]
        return response.ok


