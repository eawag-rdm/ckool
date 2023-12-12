import requests


def base_get(server, api_key):
    def _base_get(endpoint):
        url = f"{server}{endpoint}"
        headers = {'Authorization': api_key}
        r = requests.get(
            url,
            headers=headers
        )
        r.raise_for_status()
        return r.json()
    return _base_get


def base_post(server, api_key):
    def _base_get(endpoint, data, *args, **kwargs):
        url = f"{server}{endpoint}"
        headers = {
            'Authorization': api_key
        }
        r = requests.post(
            url,
            *args,
            headers=headers,
            data=data,
            **kwargs
        )
        r.raise_for_status()
        return r.json()
    return _base_get
