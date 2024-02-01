import pathlib
import shutil


def _iter_dir(path: pathlib.Path):
    for candidate in path.iterdir():
        if candidate.is_file():
            yield candidate
        else:
            yield from _iter_dir(candidate)


class LocalDoiStore:
    def __init__(self, path, top_folders_to_ignore=(".git")):
        self.path = pathlib.Path(path)
        self.ignore = top_folders_to_ignore
        if not self.path.exists():
            raise ValueError(f"The path your provided '{path}' does not exist.")

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
                if not overwrite and dst.exists():
                    raise ValueError(
                        f"The file '{dst}' already exists. To overwrite existing files use the overwrite parameter."
                    )
                with dst.open("w+") as f:
                    f.write(content)

                written.append(dst)
        return written
