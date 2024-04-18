import json
import pathlib
import re
import sys
from datetime import datetime

from rich.prompt import Prompt

from ckool import PUBLISHER
from ckool.interfaces.dora import Dora
from ckool.other.metadata_tools import prepare_metadata_for_publication_package


def mk_point_location(lon, lat):
    point_location = {
        "geoLocation": [
            {
                "geoLocationPoint": [
                    {"pointLongitude": str(lon)},
                    {"pointLatitude": str(lat)},
                ]
            }
        ]
    }
    return point_location


def split_author(author):
    last, rest = author.split(",")
    email = re.search("<(.+)>", rest)
    if email:
        email = email.groups()[0]
        first = re.sub("<(.+)>", "", rest)
    else:
        first = rest
    last = last.strip()
    first = first.strip()
    return first, last, email


def _description_parse(desc):
    # creates a list of br-elements (<children>) with appropriate tails
    # and a string <text>, representing the text-value of the parent.

    texts = desc.split("\r\n")
    text = texts.pop(0)
    children = [{"br": {"val": "", "tail": t}} for t in texts]
    return text, children


def _date_from_iso(isodate):
    return datetime.strptime(isodate, "%Y-%m-%dT%H:%M:%S.%f")


def _converttime(solrtimerange):
    # Converts SOLR date & daterange - format to RKMS-ISO8601
    if re.search(r"\s+TO\s+", solrtimerange):
        fro, to = solrtimerange.split("TO")
        fro = fro.strip()
        fro = "" if fro == "*" else fro
        to = to.strip()
        to = "" if to == "*" else to
        return "{}/{}".format(fro, to)
    else:
        isotimerange = "/" if solrtimerange.strip() == "*" else solrtimerange.strip()
        return isotimerange


class MetaDataFormatter:
    @staticmethod
    def elements():
        return [
            (1, "identifier"),
            (2, "creators"),
            (3, "titles"),
            (4, "publisher"),
            (5, "publicationYear"),
            (6, "resourceType"),
            (7, "subjects"),
            (8, "contributors"),
            (9, "dates"),
            (10, "language"),
            (11, "alternateIdentifiers"),
            (12, "relatedIdentifiers"),
            (13, "sizes"),
            (14, "formats"),
            (15, "version"),
            (16, "rightslist"),
            (17, "descriptions"),
            (18, "geolocations"),
            (19, "fundingReferences"),
        ]

    # TODO check with Stuart if resource_type is "freetext"
    def __init__(
        self,
        package_metadata: dict,
        doi: str,
        outfile: str | pathlib.Path,
        affiliations: dict = None,
        orcids: dict = None,
        related_publications: dict = None,
        resource_type: str = "Publication Data Package",
        resource_type_general: str = "Collection",
        version: str = "1.0",
    ):
        """
        package_metadata: dict,
            Metadata from ckan package from package show api call.
        doi: str,
            doi string.
        outfile: str | pathlib.Path,
            filepath for output file.
        affiliations: dict | None,
            affiliations of authors.
        orcids: dict | None,
            Orcid ids of authors, if available.
        related_publications: dict | None,
            other data publications that are related to this one.
        resource_type: str,
            Free text to describe the resource type default is 'Publication Data Package'.
        resource_type_general: str,
            resource type general (default is 'Collection') possible inputs: [
                "Audiovisual",
                "Collection",
                "Dataset",
                "Image",
                "Model",
                "Software",
                "Sound",
                "Text",
                "Other",
            ]
        version: str,
            Version of the data package, default is '1.0'.
        """
        self.package_metadata = package_metadata  # from here CKAN.get_package_metadata
        self.doi = doi
        self.output = {"resource": []}
        self.outfile = outfile
        self.affiliations = affiliations
        self.orcids = orcids
        self.related_publications = related_publications
        self.related_identifiers_from_file = self.related_publications
        self.resource_type = resource_type
        self.resource_type_general = resource_type_general
        self.version = version

        if self.orcids is None:
            self.orcids = {}

        if self.affiliations is None:
            self.affiliations = {}

        if self.related_publications is None:
            self.related_identifiers_from_file = None

    def xs_identifier(self):
        self.output["resource"].append(
            {"identifier": {"val": self.doi, "att": {"identifierType": "DOI"}}}
        )

    def xs_creators(self):
        # distinction between personal name and organization name as author
        def get_name_type(author):
            if "," in author:
                return "Personal"
            else:
                confirmation = Prompt.ask(
                    f"The Author '{author}' is not in the right format. Is it an organization?",
                    choices=["no", "yes"],
                    default="no",
                )

                if confirmation != "yes":
                    sys.exit("ABORT: illegal author name")
                else:
                    return "Organizational"

        # name-part of creator (Personal)
        def mk_creator(first, last):
            return [
                {
                    "creatorName": {
                        "val": "{}, {}".format(last, first),
                        "att": {"nameType": "Personal"},
                    }
                },
                {"givenName": first},
                {"familyName": last},
            ]

        def add_orcid(first, last, creator):
            orcid = self.orcids.get(f"{last}, {first}")
            if orcid:
                creator.append(
                    {
                        "nameIdentifier": {
                            "val": orcid,
                            "att": {
                                "nameIdentifierScheme": "ORCID",
                                "schemeURI": "https://orcid.org/",
                            },
                        }
                    }
                )

            return creator

        def add_affiliation(first, last, creator):
            if self.affiliations is not None:
                affiliation = self.affiliations.get(f"{last}, {first}")
                if affiliation:
                    creator.append({"affiliation": affiliation})
            return creator

        # main loop starts here
        creators = {"creators": []}
        for _author in self.package_metadata["author"]:
            name_type = get_name_type(_author)
            if name_type == "Organizational":
                _creator = [
                    {"creatorName": {"val": _author, "att": {"nameType": name_type}}}
                ]
            else:
                _first, _last, _email = split_author(_author)
                _creator = mk_creator(_first, _last)
                _creator = add_orcid(_first, _last, _creator)
                _creator = add_affiliation(_first, _last, _creator)

            creators["creators"].append({"creator": _creator})

        self.output["resource"].append(creators)

    def xs_titles(self):
        title = self.package_metadata["title"]
        self.output["resource"].append(
            {"titles": [{"title": {"val": title, "att": {"lang": "en"}}}]}
        )

    def xs_publisher(self):
        self.output["resource"].append({"publisher": PUBLISHER})

    def xs_publicationYear(self):
        # We assume publication happened in the same year as metadata was created.
        pubyear = _date_from_iso(self.package_metadata["metadata_created"]).year
        self.output["resource"].append({"publicationYear": str(pubyear)})

    def xs_resourceType(self):
        if self.resource_type_general not in [
            "Audiovisual",
            "Collection",
            "Dataset",
            "Image",
            "Model",
            "Software",
            "Sound",
            "Text",
            "Other",
        ]:
            raise ValueError(
                "Illegal ResourceTypeGeneral [{}]\n".format(self.resource_type_general)
            )
        self.output["resource"].append(
            {
                "resourceType": {
                    "val": self.resource_type,
                    "att": {"resourceTypeGeneral": self.resource_type_general},
                }
            }
        )

    def xs_subjects(self):
        # This has to be amended if subjects (keywords) are from
        # a specific ontology. It also needs to change if
        # CKAN metadata schema changes in any of the fields suitable
        # as keywords
        generic = self.package_metadata.get("generic-terms") or []
        taxa = self.package_metadata.get("taxa") or []
        substances = self.package_metadata.get("substances") or []
        systems = self.package_metadata.get("systems") or []
        tags = [t["display_name"] for t in self.package_metadata.get("tags")]
        keywords = generic + taxa + substances + systems + tags
        keywords = [k for k in keywords if k not in ["none"]]
        subjects = [{"subject": {"val": k, "att": {"lang": "en"}}} for k in keywords]
        self.output["resource"].append({"subjects": subjects})

    def xs_contributors(self):
        # Not implemented
        return

    def xs_dates(self):
        # We interpret CKAN's 'metadata_modified' as 'Submitted',
        # assuming that the last changes where made shortly before
        # DOI creation was requested.
        # The only other date(s) considered here are dateType=Collected
        # Other dateTyps (https://schema.datacite.org/meta/kernel-4.1
        # /include/datacite-dateType-v4.1.xsd) would have to be added.

        # Also: Everything is UTC everywhere.
        submitted = _date_from_iso(self.package_metadata["metadata_modified"])
        submitted = [
            {
                "date": {
                    "val": submitted.strftime("%Y-%m-%d"),
                    "att": {"dateType": "Submitted"},
                }
            }
        ]
        collected = [
            {"date": {"val": _converttime(t), "att": {"dateType": "Collected"}}}
            for t in self.package_metadata["timerange"]
        ]

        self.output["resource"].append({"dates": submitted + collected})

    def xs_language(self):
        # We assume an anglophonic world
        self.output["resource"].append({"language": "en"})

    def xs_alternateIdentifiers(self):
        # Not implemented
        return

    def xs_relatedIdentifiers(self):
        # We scan the 'description' field of all resources
        # for a simple custom format
        descriptions = [
            (r.get("url"), r.get("description"), r.get("resource_type"))
            for r in self.package_metadata["resources"]
        ]
        relatedIdentifiers = []
        for d in descriptions:
            if (
                d[1] is not None
            ):  # TODO This allows empty resource descriptions, check with Stuart
                lines = re.split(r"\s*\r\n", d[1])
                lines = [line.strip() for line in lines]
                if lines[0] == "relatedIdentifier":
                    rel_types = re.sub(r"relationTypes:\s*", "", lines[2])
                    rel_types = rel_types.split(",")
                    rel_types = [rt.strip() for rt in rel_types]
                    rel_id_type = re.sub(r"relatedIdentifierType:\s*", "", lines[1])
                    rel_id_type = rel_id_type.strip()

                    relatedIdentifiers += [
                        {
                            "relatedIdentifier": {
                                "val": d[0],
                                "att": {
                                    "resourceTypeGeneral": d[2],
                                    "relatedIdentifierType": rel_id_type,
                                    "relationType": rt,
                                },
                            }
                        }
                        for rt in rel_types
                    ]

        if self.related_identifiers_from_file:
            relatedIdentifiers += self.related_identifiers_from_file
        else:
            publicationlink = self.package_metadata.get("publicationlink")
            if publicationlink:
                paperdoi = Dora.doi_from_publication_link(publicationlink)
                relatedIdentifiers += [
                    {
                        "relatedIdentifier": {
                            "val": f"{paperdoi}",
                            "att": {
                                "resourceTypeGeneral": "Text",
                                "relatedIdentifierType": "DOI",
                                "relationType": "IsSupplementTo",
                            },
                        }
                    },
                    {
                        "relatedIdentifier": {
                            "val": f"{paperdoi}",
                            "att": {
                                "resourceTypeGeneral": "Text",
                                "relatedIdentifierType": "DOI",
                                "relationType": "IsSupplementedBy",
                            },
                        }
                    },
                ]
        if relatedIdentifiers:
            self.output["resource"].append({"relatedIdentifiers": relatedIdentifiers})

    def xs_sizes(self):
        # Not implemented
        return

    def xs_formats(self):
        # Not implemented
        return

    def xs_version(self):
        self.output["resource"].append({"version": self.version})

    def xs_rightslist(self):
        self.output["resource"].append(
            {
                "rightsList": [
                    {
                        "rights": {
                            "val": "CC0 1.0 Universal (CC0 1.0) "
                            "Public Domain Dedication",
                            "att": {
                                "rightsURI": "https://creativecommons.org/publicdomain"
                                "/zero/1.0/",
                                "lang": "en",
                            },
                        }
                    }
                ]
            }
        )

    def xs_descriptions(self):
        # We only consider descriptionType "Abstract"
        abstract = self.package_metadata["notes"]
        text, children = _description_parse(abstract)
        descriptions = {
            "descriptions": [
                {
                    "description": {
                        "val": text,
                        "att": {"descriptionType": "Abstract", "lang": "en"},
                        "children": children,
                    }
                }
            ]
        }
        self.output["resource"].append(descriptions)

    def xs_geolocations(self):
        # TODO Check how this deals with Polygon. Is this in line with what the frontend supports?
        # Currently only implemented:
        # + geoLocationPoint
        # + geoLocationPlace
        # + geoLocation - MultiPoint
        #
        # Note that CKAN notation is lon/lat.
        #
        # Each geoLocation-feature (place, point) is one geoLocation.
        # The spec seems to allow to accociate, say a geoname and a point,
        # but we can't do that in CKAN anyway, and I don't really understand
        # the XML (xs:choice).

        geo_locations = []

        geonames = self.package_metadata.get("geographic_name")
        for nam in geonames:
            geo_locations.append({"geoLocation": [{"geoLocationPlace": nam}]})

        spatial = self.package_metadata.get("spatial")
        # Old eric has default of '{}' which represents empty
        if spatial == "{}":
            spatial = ""
        if spatial:
            spatial = json.loads(self.package_metadata.get("spatial"))
            if spatial["type"] == "Point":
                lon = spatial["coordinates"][0]
                lat = spatial["coordinates"][1]
                geo_locations.append(mk_point_location(lon, lat))

            if spatial["type"] == "MultiPoint":
                for point in spatial["coordinates"]:
                    lon = point[0]
                    lat = point[1]
                    geo_locations.append(mk_point_location(lon, lat))

        if geo_locations:
            self.output["resource"].append({"geoLocations": geo_locations})

    def xs_fundingReferences(self):
        # Not implemented
        return

    def prepare_raw_for_publication(
        self,
        maintainer_record: dict,
        usage_contact_record: dict,
        custom_citation_publication: str = None,
    ):
        self.package_metadata = prepare_metadata_for_publication_package(
            pkg=self.package_metadata,
            doi=self.doi,
            maintainer_record=maintainer_record,
            usage_contact_record=usage_contact_record,
            custom_citation_publication=custom_citation_publication,
        )

    def main(self):
        funcnames = ["xs_{}".format(e[1]) for e in self.elements()]
        for f in funcnames:
            getattr(self, f)()
        with open(self.outfile, "w") as f_out:
            json.dump(self.output, f_out, indent=2)

        return self.output
