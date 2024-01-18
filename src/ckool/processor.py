import pathlib

from .file_management import generate_archive_dest, glob_files, tar_files, zip_files
from .hashing import get_hash_func


class LocalProcessor:
    def __init__(
        self,
        package_root_path: pathlib.Path,
        file_path: pathlib.Path,
        hash_type: str,
        processing_folder_name: str = ".ckool",
    ):
        self.package_root_path = package_root_path
        self.file_path = file_path
        self.hash_type = hash_type
        self.processing_folder_name = processing_folder_name

    def get_hash(self):
        hash_func = get_hash_func(self.hash_type)
        return hash_func(self.file_path)

    def get_size(self):
        return self.file_path.stat().st_size

    def compress(self, compression_type):
        if not self.file_path.is_dir():
            raise ValueError(
                "Invalid configuration: A file does not need to be compressed."
            )

        compression_algorithms = {"zip": zip_files, "tar": tar_files}

        archive_destination = generate_archive_dest(
            folder_to_zip=self.file_path,
            root_folder=self.package_root_path,
            tmp_dir_name=self.processing_folder_name,
        )

        compression_algorithms[compression_type](
            self.file_path,
            archive_destination,
            [file for file in glob_files(self.file_path)],
        )

    def process(self, config: list):
        """
        config: [{compress: zip | tar}, hash, size]
        """
