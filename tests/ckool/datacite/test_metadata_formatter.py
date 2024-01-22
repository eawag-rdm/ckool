import json

from ckool.datacite.metadata_formatter import MetaDataFormatter


def test_init_metadata_formatter(tmp_path, json_test_data):
    MetaDataFormatter(
        doi="this is some doi", outfile=tmp_path / "out.txt", **json_test_data
    )


def test_meta_data_formatter_main(tmp_path, json_test_data, valid_outputs):
    MetaDataFormatter(
        doi="this is some doi", outfile=tmp_path / "out.txt", **json_test_data
    ).main()

    with (tmp_path / "out.txt").open() as written_output, (
        valid_outputs / "metadataformatter.json"
    ).open() as valid:
        assert json.load(written_output) == json.load(valid)
