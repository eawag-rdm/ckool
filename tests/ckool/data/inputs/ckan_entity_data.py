organization_data = {
    "title": "Test_Organization",
    "description": "This is my organization.",
    "homepage": "https://www.eawag.ch/de/",
    "datamanager": "ckan_admin",
    "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
}

package_data = {
    "title": "Test_Package",
    "private": False,
    "description": "This is my package.",
    "author": "admin, ckan",
    "author_email": "ckan.admin@localhost.ch",
    "state": "active",
    "type": "dataset",
    "owner_org": "test_organization",
    "reviewed_by": "",
    "maintainer": "ckan_admin",
    "maintainer_email": "ckan.admin@domain.com",  # required by eric open
    "usage_contact": "ckan_admin",
    "notes": "some_note",
    "review_level": "none",
    "spatial": '{"type": "Point", "coordinates": [8.609776496939471, 47.40384502816517]}',
    "status": "incomplete",
    "tags_string": "some_tag",
    "timerange": "*",
    "variables": "none",
}

resource_data = {
    "resource_type": "Dataset",
    "restricted_level": "public",
    "description": "this is a link",
    "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
}


project_data = {
    "approval_status": "approved",
    "created": "2024-02-16T11:27:07.145278",
    "description": "This is a test project",
    "display_name": "test_project",
    "image_display_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
    "image_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
    "is_organization": False,
    "num_followers": 0,
    "package_count": 0,
    "title": "test_project",
    "type": "group",
    "users": [],
    "extras": [],
    "tags": [],
    "groups": [],
}


test_resources = [
    {
        "cache_last_updated": None,
        "cache_url": None,
        "created": "2024-02-19T17:40:03.085674",
        "datastore_active": False,
        "description": None,
        "format": "JPEG",
        "hash": "",
        "id": "0b481119-6275-44fc-a8cd-b8ede5878786",
        "last_modified": None,
        "metadata_modified": "2024-02-19T17:40:03.489792",
        "mimetype": "image/jpeg",
        "mimetype_inner": None,
        "name": "test_resource_link",
        "package_id": "5e0c1fb8-3d03-4d9e-8b59-3626ddc84493",
        "position": 0,
        "resource_type": "Dataset",
        "restricted_level": "public",
        "size": None,
        "state": "active",
        "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
        "url_type": None,
    },
    {
        "cache_last_updated": None,
        "cache_url": None,
        "citation": "",
        "created": "2024-02-19T17:40:03.495710",
        "datastore_active": False,
        "description": "",
        "format": "",
        "hash": "fake-hash-to-save-time",
        "hashtype": "sha256",
        "id": "f44fbccb-28fc-448c-9210-883f5ead53e9",
        "last_modified": "2024-02-19T17:40:03.472381",
        "metadata_modified": "2024-02-19T17:40:03.827869",
        "mimetype": None,
        "mimetype_inner": None,
        "name": "file_0",
        "package_id": "5e0c1fb8-3d03-4d9e-8b59-3626ddc84493",
        "position": 1,
        "resource_type": "Dataset",
        "restricted_level": "public",
        "size": 1048576,
        "state": "active",
        "url": "https://localhost:8443/dataset/5e0c1fb8-3d03-4d9e-8b59-3626ddc84493/resource/f44fbccb-28fc-448c-9210-883f5ead53e9/download/file_0",
        "url_type": "upload",
    },
]


full_package_data = {
    "author": ["this is a free text"],
    "author_email": "example@localhost.ch",
    "creator_user_id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
    "id": "bfc07875-6cad-4af7-a5e9-e7318955c0fc",
    "isopen": False,
    "license_id": None,
    "license_title": None,
    "maintainer": "ckan_admin",
    "maintainer_email": None,
    "metadata_created": "2024-02-19T14:33:50.683412",
    "metadata_modified": "2024-02-19T14:33:50.974051",
    "name": "new_test_package",
    "notes": "some_note",
    "num_resources": 1,
    "num_tags": 1,
    "organization": {
        "id": "1924b2db-e39f-42c2-a640-7709b98bf95f",
        "name": "test_organization",
        "title": "Test_Organization",
        "type": "organization",
        "description": "This is my organization.",
        "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
        "created": "2024-02-19T14:33:50.593412",
        "is_organization": True,
        "approval_status": "approved",
        "state": "active",
    },
    "owner_org": "THIS NEEDS TO BE GOTTEN",
    "private": False,
    "publicationlink": "",
    "review_level": "none",
    "reviewed_by": "",
    "spatial": '{"type": "Point", "coordinates": [8.609776496939471, 47.40384502816517]}',
    "state": "active",
    "status": "incomplete",
    "tags_string": "some_tag",
    "timerange": ["*"],
    "title": "Test_Package",
    "type": "dataset",
    "url": None,
    "usage_contact": "ckan_admin",
    "variables": ["none"],
    "version": None,
    "resources": [
        {
            "cache_last_updated": None,
            "cache_url": None,
            "created": "2024-02-19T14:33:50.982168",
            "datastore_active": False,
            "description": None,
            "format": "JPEG",
            "hash": "",
            "id": "a63195ed-7802-4193-b24e-fec209d55ecc",
            "last_modified": None,
            "metadata_modified": "2024-02-19T14:33:50.978578",
            "mimetype": "image/jpeg",
            "mimetype_inner": None,
            "name": "test_resource_link",
            "package_id": "bfc07875-6cad-4af7-a5e9-e7318955c0fc",
            "position": 0,
            "resource_type": "Dataset",
            "restricted_level": "public",
            "size": None,
            "state": "active",
            "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
            "url_type": None,
        }
    ],
    "tags": [
        {
            "display_name": "some_tag",
            "id": "817c946f-d18e-493a-a2b7-ff22d1e4d650",
            "name": "some_tag",
            "state": "active",
            "vocabulary_id": None,
        }
    ],
    "groups": [
        {
            "description": "This is a test project",
            "display_name": "test_project",
            "id": "THIS NEEDS TO BE GOTTEN",
            "image_display_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
            "name": "test_group",
            "title": "test_project",
        }
    ],
    "relationships_as_subject": [],
    "relationships_as_object": [],
}


full_project_data = {
    "approval_status": "approved",
    "created": "2024-02-19T14:31:11.072921",
    "description": "This is a test project",
    "display_name": "test_project",
    "id": "d031afd5-8de5-4b23-aa98-cef196f38614",
    "image_display_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
    "image_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
    "is_organization": False,
    "name": "new project name",
    "num_followers": 0,
    "package_count": 0,
    "state": "active",
    "title": "test_project",
    "type": "group",
    "users": [
        {
            "about": None,
            "activity_streams_email_notifications": False,
            "capacity": "admin",
            "created": "2024-01-25T09:33:37.949157",
            "display_name": "ckan admin",
            "email_hash": "7c512b13badc48258d94ef72d1c8889a",
            "fullname": None,
            "id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
            "image_display_url": None,
            "image_url": None,
            "name": "ckan_admin",
            "number_created_packages": 1,
            "state": "active",
            "sysadmin": True,
        }
    ],
    "extras": [],
    "packages": [],
    "tags": [],
    "groups": [],
}


full_organization_data = {
    "approval_status": "approved",
    "created": "2024-02-19T14:32:30.816151",
    "datamanager": "ckan_admin",
    "description": "This is my organization.",
    "display_name": "Test_Organization",
    "homepage": "https://www.eawag.ch/de/",
    "id": "e82850c4-aca0-4060-ab01-486ad305fb10",
    "image_display_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
    "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
    "is_organization": True,
    "name": "new_org_name",
    "num_followers": 0,
    "package_count": 1,
    "state": "active",
    "title": "Test_Organization",
    "type": "organization",
    "users": [
        {
            "about": None,
            "activity_streams_email_notifications": False,
            "capacity": "admin",
            "created": "2024-01-25T09:33:37.949157",
            "display_name": "ckan admin",
            "email_hash": "7c512b13badc48258d94ef72d1c8889a",
            "fullname": None,
            "id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
            "image_display_url": None,
            "image_url": None,
            "name": "ckan_admin",
            "number_created_packages": 1,
            "state": "active",
            "sysadmin": True,
        }
    ],
    "tags": [],
    "groups": [],
}
