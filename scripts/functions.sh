#!/bin/bash

# Function to get API key
get_apikey() {
    user="$1"
    host="$2"
    ssh_key_path="$3"
    ckan_username="$4"

    ssh "$user@$host" -i "$ssh_key_path" "docker exec -u postgres db psql -d ckandb -c \"SELECT apikey FROM public.user WHERE name='$ckan_username';\"" | grep -o '[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}'
}

