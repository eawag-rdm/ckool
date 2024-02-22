from enum import Enum


class CompressionTypes(Enum):
    zip = "zip"
    tar_gz = "tar.gz"
    tar_xz = "tar.xz"
    tar_bz2 = "tar.bz2"


class HashTypes(Enum):
    """
        Currently CKAN only supports hashes of type 'md5' and 'sha256'
        hashing with a different type will result in this error:
        ValidationError: {'hashtype': ["Hashtype must be one of ['md5', 'sha256']"], '__type': 'Validation
    Error'}
    """

    md5 = "md5"
    # sha1 = "sha1"
    # sha224 = "sha224"
    sha256 = "sha256"
    # sha512 = "sha512"
