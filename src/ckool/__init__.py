from ckool.other.types import CompressionTypes, HashTypes

TEMPORARY_DIRECTORY_NAME = ".tmp_ckan_tool"
LOGGER = None
HASH_TYPE = HashTypes.sha256
HASH_BLOCK_SIZE = 65536
COMPRESSION_TYPE = CompressionTypes.zip
OVERWRITE_FILE_STATS = True
DOWNLOAD_CHUNK_SIZE = 8192
EMPTY_FILE_NAME = "empty_file.empty"
PACKAGE_META_DATA_FILE_ENDING = ".json.meta"
UPLOAD_FUNC_FACTOR = 4.8

LOCAL_DOI_STORE_FOLDERS_TO_IGNORE = (".git",)
LOCAL_DOI_STORE_DOI_FILE_NAME = "doi.txt"
