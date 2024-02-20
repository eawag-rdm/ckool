from enum import Enum


class CompressionTypes(Enum):
    zip = "zip"
    tar_gz = "tar.gz"
    tar_xz = "tar.xz"
    tar_bz2 = "tar.bz2"


class HashTypes(Enum):
    md5 = "md5"
    sha1 = "sha1"
    sha224 = "sha224"
    sha256 = "sha256"
    sha512 = "sha512"
