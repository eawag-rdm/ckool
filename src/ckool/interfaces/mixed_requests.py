import re

import requests


def get_citation_from_doi(doi, prefix=10.25678):
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
