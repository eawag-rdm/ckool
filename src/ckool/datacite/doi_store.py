import json
import pathlib
import re
import shutil

from ckool import (
    LOCAL_DOI_STORE_AFFILIATION_FILE_NAME,
    LOCAL_DOI_STORE_DOI_FILE_NAME,
    LOCAL_DOI_STORE_FOLDERS_TO_IGNORE,
    LOCAL_DOI_STORE_METADATA_XML_FILE_NAME,
    LOCAL_DOI_STORE_ORCIDS_FILE_NAME,
    LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME,
)


def _iter_dir(path: pathlib.Path):
    for candidate in path.iterdir():
        if candidate.is_file():
            yield candidate
        else:
            yield from _iter_dir(candidate)


def retrieve_doi_from_doi_file(package_name: str, file: pathlib.Path):
    content = file.read_text()
    dois = re.findall(r"10\.[0-9]{4,5}/[0-9A-Z]{6}", content)
    if len(set(dois)) > 1:
        raise ValueError(
            f"Conflicting dois for package '{package_name}' found in file '{file.as_posix()}':\n{set(dois)}"
        )
    if not dois:
        raise ValueError(
            f"No dois for package '{package_name}' found in file '{file.as_posix()}'."
        )
    return dois[0]


class LocalDoiStore:
    def __init__(
        self,
        path: str | pathlib.Path,
        top_folders_to_ignore: str = LOCAL_DOI_STORE_FOLDERS_TO_IGNORE,
    ):
        self.path = pathlib.Path(path) if isinstance(path, str) else path
        self.ignore = top_folders_to_ignore
        self.doi_store_package_location = {}
        if not self.path.exists():
            raise ValueError(f"The path your provided '{path}' does not exist.")

    def _find_doi_store_package_location(self, package_name):
        if path := self.doi_store_package_location.get(package_name):
            return path

        found_name = None
        for name in self.path.iterdir():
            if name.is_file():
                continue
            for package in name.iterdir():
                if package.name == package_name:
                    found_name = name
                    break
        if found_name is None:
            raise ValueError(
                f"The package '{package_name}' you're referring to can not be found in the datastore."
            )
        self.doi_store_package_location[package_name] = (
            self.path / found_name / package_name
        )
        return self.doi_store_package_location.get(package_name)

    def generate_xml_filepath(self, package_name) -> pathlib.Path:
        return (
            self._find_doi_store_package_location(package_name)
            / LOCAL_DOI_STORE_METADATA_XML_FILE_NAME
        )

    def generate_affiliations_filepath(self, package_name) -> pathlib.Path:
        return (
            self._find_doi_store_package_location(package_name)
            / LOCAL_DOI_STORE_AFFILIATION_FILE_NAME
        )

    def generate_orcids_filepath(self, package_name) -> pathlib.Path:
        return (
            self._find_doi_store_package_location(package_name)
            / LOCAL_DOI_STORE_ORCIDS_FILE_NAME
        )

    def generate_related_publications_filepath(self, package_name) -> pathlib.Path:
        return (
            self._find_doi_store_package_location(package_name)
            / LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME
        )

    def parse(self):
        basic_map = {"other": []}
        for file in _iter_dir(self.path):
            relative_file = file.relative_to(self.path)
            parts = relative_file.parts

            if parts[0] in self.ignore:
                continue

            if len(parts) == 1:
                basic_map["other"].append(relative_file.as_posix())
                continue

            name, package, *other = parts

            if name not in basic_map:
                basic_map[name] = {}
            if package not in basic_map[name]:
                basic_map[name][package] = []

            basic_map[name][package].append("/".join(other))
        return basic_map

    def _find_file(self, package_name: str, filename: str, raise_error: bool = True):
        found = False
        for file in _iter_dir(self.path):
            if package_name in file.as_posix() and file.name == filename:
                return file

        if not found and raise_error:
            raise FileNotFoundError(
                f"No doi file '{filename}' for package '{package_name}' could be found."
            )

    @staticmethod
    def __return_file_content(file):
        if file:
            with file.open() as f:
                return json.load(f)
        return None

    def get_doi(self, package_name: str, filename: str = LOCAL_DOI_STORE_DOI_FILE_NAME):
        # TODO improve, dont search is doi store package dir was already detected.
        file = self._find_file(package_name, filename, raise_error=True)
        return retrieve_doi_from_doi_file(package_name, file)

    def get_orcids(
        self, package_name: str, filename: str = LOCAL_DOI_STORE_ORCIDS_FILE_NAME
    ):
        file = self._find_file(package_name, filename, raise_error=False)
        return self.__return_file_content(file)

    def get_affiliations(
        self, package_name: str, filename: str = LOCAL_DOI_STORE_AFFILIATION_FILE_NAME
    ):
        file = self._find_file(package_name, filename, raise_error=False)
        return self.__return_file_content(file)

    def get_related_publications(
        self,
        package_name: str,
        filename: str = LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME,
    ):
        file = self._find_file(package_name, filename, raise_error=False)
        return self.__return_file_content(file)

    def get_xml_file(
        self, package_name: str, filename: str = LOCAL_DOI_STORE_METADATA_XML_FILE_NAME
    ):
        return self._find_file(package_name, filename, raise_error=True)

    def write(
        self,
        name: str,
        package: str,
        files: list[pathlib.Path] = None,
        filename_content_map: dict[str, str] = None,
        overwrite: bool = False,
    ):
        """
        files: list,
            a list of files to copy tp the doi store
        filename_content_map: dict,
            a dictionary mapping filenames to their content. The files will be written.
        """
        pkg_path = self.path / name / package
        pkg_path.mkdir(parents=True, exist_ok=True)

        if (
            files is not None
            and filename_content_map is not None
            or files is None
            and filename_content_map is None
        ):
            raise ValueError(
                f"You must provide one (only), either a list of files via the files parameter or "
                f"a dictionary mapping filenames to their content. "
                f"You provided:\nfiles: {repr(files)}\nfilename_content_map: {repr(filename_content_map)}"
            )

        written = []
        if files is not None:
            for file in files:
                dst = pkg_path / file.name
                if not overwrite and dst.exists():
                    raise ValueError(
                        f"The file '{dst} 'already exists. To overwrite existing files use the overwrite parameter."
                    )
                shutil.copyfile(file, dst)

                written.append(dst)

        elif filename_content_map is not None:
            for filename, content in filename_content_map.items():
                dst = pkg_path / filename
                dst = pkg_path / filename
                if not overwrite and dst.exists():
                    raise ValueError(
                        f"The file '{dst}' already exists. To overwrite existing files use the overwrite parameter."
                    )
                with dst.open("w+") as f:
                    f.write(content)

                written.append(dst)
        return written
