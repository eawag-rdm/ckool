"""
This script will use the API key of another user to create a package for them
"""

from ckool.ckan.ckan import CKAN

if __name__ == "__main__":
    ckan = CKAN(
        server="...",
        token="...",  # token of user you're creating for
        verify_certificate=False,
    )

    # retrieve a sample package
    # print(json.dumps(ckan.get_package(package_name="cowwid_raw_data_dpcr_n1-mhv_assay-2023-02"), indent=4))

    data = {
        "author": [
            "Julian, Timothy R. tim.julian@eawag.ch",
            "Ort, Christoph christoph.ort@eawag.ch",
            "Kohn, Tamar tamar.kohn@epfl.ch",
            "Caduff, Lea Lea.Caduff@eawag.ch",
            "Gan, Charlie Charlie.Gan@eawag.ch",
            "Devaux, A.J. Alexander.Devaux@eawag.ch",
            "Holschneider, Aur\u00e9lie aurelie.holschneider@eawag.ch",
            "Kang, Seju seju.kang@eawag.ch",
            "Dauletova, Ayazhan ayazhan.dauletova@eawag.ch",
            "Habl\u00fctzel, Camille camille.habluetzel@eawag.ch",
            "McLeod, Rachel rachel.mcleod@eawag.ch",
        ],
        "author_email": None,
        "creator_user_id": "4a66df76-b0f1-41d1-b81e-a1dbdce64037",
        "geographic_name": [
            "ARA Altenrhein",
            "ARA Chur",
            "ARA Sensetal/Laupen",
            "ARA Werdh\u00f6lzli",
            "CDA Lugano",
            "STEP Aire",
        ],
        "isopen": False,
        "license_id": None,
        "license_title": None,
        "maintainer": "mcleodra",
        "maintainer_email": None,
        "metadata_created": "2023-04-05T08:35:31.509492",
        "metadata_modified": "2023-04-11T05:13:32.139822",
        "name": "cowwid_raw_data_dpcr_resp6_assay-2024-01",
        "notes": "Raw ddPCR data from February 2023 for the CoWWID project. N1MHV duplex assay results. Samples run were from six different wastewater treatment plants (WWTPs) throughout Switzerland.\r\nThe data can be look at on a computer with CrystalMiner and CrystalReader programs (naica\u00ae system).",
        "notes-2": "",
        "num_resources": 38,
        "num_tags": 5,
        "open_data": "true",
        "organization": {
            "id": "0ae980fc-312e-47ce-907d-0603a178b4cf",
            "name": "pathogens-and-human-health",
            "title": "Pathogens and Human Health",
            "type": "organization",
            "description": "Our research agenda is to reduce global infectious disease burden through the study of pathogen transmission at the boundary between humans and the environment.",
            "image_url": "https://www.eawag.ch/fileadmin/Domain1/Abteilungen/umik/projekte/krankheitserreger/20180725_Thrasher_Teaser.jpg",
            "created": "2018-11-14T14:47:22.277786",
            "is_organization": True,
            "approval_status": "approved",
            "state": "active",
        },
        "owner_org": "0ae980fc-312e-47ce-907d-0603a178b4cf",
        "private": False,
        "publicationlink": "",
        "review_level": "none",
        "reviewed_by": "",
        "spatial": '{"type": "MultiPoint",     "coordinates": [ [9.567155,47.490022], [9.529527,46.870354], [7.236975,46.913004], [8.481231,47.400656], [8.917439,46.009391], [6.088355,46.196795] ]}',
        "state": "active",
        "status": "incomplete",
        "substances": [],
        "substances_generic": [],
        "systems": ["Sewage systems"],
        "tags_string": "RT-dPCR assay,dPCR,Municipal wastewater,SARS-CoV-2,virus",
        "taxa": [],
        "taxa_generic": ["Murine Hepatitis Virus (MHV)", "SARS-CoV-2"],
        "timerange": ["2023-02"],
        "title": "Cowwid_raw_data_dPCR_RESP6_assay-2024-01",
        "type": "dataset",
        "url": None,
        "usage_contact": "julianti",
        "variables": ["gene_abundance"],
        "version": None,
        "tags": [
            {
                "display_name": "Municipal wastewater",
                "id": "51a86776-c789-40cb-8988-6126aaecb011",
                "name": "Municipal wastewater",
                "state": "active",
                "vocabulary_id": None,
            },
            {
                "display_name": "RT-dPCR assay",
                "id": "e2728a53-dd7a-4711-865a-895c8f18b791",
                "name": "RT-dPCR assay",
                "state": "active",
                "vocabulary_id": None,
            },
            {
                "display_name": "SARS-CoV-2",
                "id": "acbf3237-4b42-4ff2-9105-71b9bb126e5c",
                "name": "SARS-CoV-2",
                "state": "active",
                "vocabulary_id": None,
            },
            {
                "display_name": "dPCR",
                "id": "7a18505b-161b-43c7-b35a-47c1c73eb045",
                "name": "dPCR",
                "state": "active",
                "vocabulary_id": None,
            },
            {
                "display_name": "virus",
                "id": "8ecb7c2e-38ab-44d1-94be-9c4f90cc9787",
                "name": "virus",
                "state": "active",
                "vocabulary_id": None,
            },
        ],
    }

    ckan.create_package(**data)
