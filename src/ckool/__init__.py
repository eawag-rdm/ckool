import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from ckool.other.logger import MainLogger
from ckool.other.types import CompressionTypes, HashTypes

DEFAULT_TOML_NAME = ".ckool.toml"
TEMPORARY_DIRECTORY_NAME = ".tmp_ckan_tool"
LOGGER = MainLogger()
HASH_TYPE = HashTypes.sha256
HASH_BLOCK_SIZE = 65536
COMPRESSION_TYPE = CompressionTypes.zip
OVERWRITE_FILE_STATS = True
DOWNLOAD_CHUNK_SIZE = 8192
PACKAGE_META_DATA_FILE_ENDING = ".json.meta"
PUBLICATION_INTEGRITY_CHECK_CACHE = "integrity-check-cache.json"
UPLOAD_FUNC_FACTOR = 4.8
UPLOAD_IN_PROGRESS_STRING = "-- scp overwrite in progress --"

LOCAL_DOI_STORE_FOLDERS_TO_IGNORE = (".git",)
LOCAL_DOI_STORE_DOI_FILE_NAME = "doi.txt"
LOCAL_DOI_STORE_AFFILIATION_FILE_NAME = "affiliations.json"
LOCAL_DOI_STORE_ORCIDS_FILE_NAME = "orcids.json"
LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME = "related_publications.json"
LOCAL_DOI_STORE_METADATA_XML_FILE_NAME = "metadata.xml"

DEFAULT_AFFILIATION = "Eawag: Swiss Federal Institute of Aquatic Science and Technology"
PUBLISHER = "Eawag: Swiss Federal Institute of Aquatic Science and Technology"
