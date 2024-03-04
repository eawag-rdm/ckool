import pathlib

import pytest

from ckool.datacite.parse_datacite_schema import SchemaParser


@pytest.mark.parametrize(
    "typ,choices",
    (
        (
            "relatedIdentifierType",
            [
                "ARK",
                "arXiv",
                "bibcode",
                "DOI",
                "EAN13",
                "EISSN",
                "Handle",
                "IGSN",
                "ISBN",
                "ISSN",
                "ISTC",
                "LISSN",
                "LSID",
                "PMID",
                "PURL",
                "UPC",
                "URL",
                "URN",
            ],
        ),
        (
            "resourceType",
            [
                "Audiovisual",
                "Book",
                "BookChapter",
                "Collection",
                "ComputationalNotebook",
                "ConferencePaper",
                "ConferenceProceeding",
                "DataPaper",
                "Dataset",
                "Dissertation",
                "Event",
                "Image",
                "InteractiveResource",
                "Journal",
                "JournalArticle",
                "Model",
                "OutputManagementPlan",
                "PeerReview",
                "PhysicalObject",
                "Preprint",
                "Report",
                "Service",
                "Software",
                "Sound",
                "Standard",
                "Text",
                "Workflow",
                "Other",
            ],
        ),
        (
            "relationType",
            [
                "IsCitedBy",
                "Cites",
                "IsSupplementTo",
                "IsSupplementedBy",
                "IsContinuedBy",
                "Continues",
                "IsNewVersionOf",
                "IsPreviousVersionOf",
                "IsPartOf",
                "HasPart",
                "IsPublishedIn",
                "IsReferencedBy",
                "References",
                "IsDocumentedBy",
                "Documents",
                "IsCompiledBy",
                "Compiles",
                "IsVariantFormOf",
                "IsOriginalFormOf",
                "IsIdenticalTo",
                "HasMetadata",
                "IsMetadataFor",
                "Reviews",
                "IsReviewedBy",
                "IsDerivedFrom",
                "IsSourceOf",
                "Describes",
                "IsDescribedBy",
                "HasVersion",
                "IsVersionOf",
                "Requires",
                "IsRequiredBy",
                "Obsoletes",
                "IsObsoletedBy",
            ],
        ),
    ),
)
def test_get_schema_choices(typ, choices):
    sp = SchemaParser(
        pathlib.Path(__file__).parent.parent.parent.parent
        / "src"
        / "ckool"
        / "datacite"
        / "schema"
        / "datacite"
        / "metadata_schema_4.5.xsd"
    )
    assert sp.get_schema_choices(typ) == choices
