#!/bin/sh

# generate token
#TOKEN=$(docker exec ckan ckan user token add ckan_admin test_token 2>/dev/null | grep -v "API" | tr -d "[:space:]")

# Create API
docker exec -u postgres -it db psql -d ckandb -c "UPDATE public.user SET apikey='b4ff5ef3-c61c-6e9f-1cd7-ebf2ec35ae3c' WHERE name='ckan_admin' AND sysadmin='t';"

API_URL="https://localhost:8443"
TOKEN="b4ff5ef3-c61c-6e9f-1cd7-ebf2ec35ae3c"
ORGANIZATION_NAME="test_organization"
PACKAGE_NAME="test_package"

# Create test_organization
curl --insecure -X POST "${API_URL}/api/3/action/organization_create" \
     -H "Content-Type: application/json" \
     -H "Authorization: $TOKEN" \
     -d '{
           "name": "'"$ORGANIZATION_NAME"'",
           "title": "Test_Organization",
           "description": "This is my organization.",
           "homepage": "https://www.eawag.ch/de/",
           "datamanager": "ckan_admin",
           "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg"
         }'

# Create test_package
curl --insecure -X POST "${API_URL}/api/3/action/package_create" \
     -H "Content-Type: application/json" \
     -H "Authorization: $TOKEN" \
     -d '{
           "name": "'"$PACKAGE_NAME"'",
           "title": "Test_Package",
           "private": false,
           "description": "This is my package.",
           "author": "ckan_admin",
           "author_email": "example@localhost.ch",
           "state": "active",
           "type": "dataset",
           "owner_org": "'"$ORGANIZATION_NAME"'",
           "reviewed_by": "",
           "maintainer": "ckan_admin",
           "usage_contact": "ckan_admin",
           "notes": "some_note",
           "review_level": "none",
           "spatial": "{\"type\": \"Point\", \"coordinates\": [8.609776496939471, 47.40384502816517]}",
           "status": "incomplete",
           "tags_string": "some_tag",
           "timerange": "*",
           "variables": "none"
         }'

# Create test_resource
curl --insecure -X POST "${API_URL}/api/3/action/resource_create" \
     -H "Content-Type: application/json" \
     -H "Authorization: $TOKEN" \
     -d '{
           "package_id": "'"$PACKAGE_NAME"'",
           "name": "test_resource",
           "resource_type": "Dataset",
           "restricted_level": "public",
           "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg"
         }'


fallocate -l 50M large_file.img
# create resource_file
curl --insecure -X POST "${API_URL}/api/3/action/resource_create" \
     -H "Content-Type: multipart/form-data" \
     -H "Authorization: ${TOKEN}" \
     -F "package_id=${PACKAGE_NAME}" \
     -F "name=test_resource_file" \
     -F "resource_type=Dataset" \
     -F "restricted_level=public" \
     -F "upload=@large_file.img"
