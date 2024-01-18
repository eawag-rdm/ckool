import ckanapi

from ..utilities import get_secret


class CKAN:
    def __init__(self, server, apikey=None, secret=None):
        self.server = server
        self.apikey = apikey if apikey is not None else get_secret(secret)

    def get_package_metadata(self, package_name):
        with ckanapi.RemoteCKAN(self.server, apikey=self.apikey) as conn:
            return conn.call_action("package_show", {"id": package_name})
