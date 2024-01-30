#!/bin/sh

# generate token
#TOKEN=$(docker exec ckan ckan user token add ckan_admin test_token 2>/dev/null | grep -v "API" | tr -d "[:space:]")

# Create API KEY
docker exec -u postgres -it db psql -d ckandb -c "UPDATE public.user SET apikey='b4ff5ef3-c61c-6e9f-1cd7-ebf2ec35ae3c' WHERE name='ckan_admin' AND sysadmin='t';"
