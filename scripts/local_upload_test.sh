# upload fake resource then replace on filesystem
storage_path=/var/lib/ckan/ckan-eric-int/resources
api_key=

get_url() {
  local response="$1"
  echo "$response" | grep -oP '"url": "\K[^"]+' | sed 's/\\//g'
}

get_resource_string() {
  local url="$1"
  echo "$url" | grep -oP "(?<=resource/)[^/]+(?=/download)"
}

get_resource_path() {
  local resource_string="$1"
  local storage_path="$2"
  echo "$resource_string" | awk -v path="$storage_path" -F'-' '{print path "/" substr($1, 1, 3) "/" substr($1, 4, 3) "/" substr($0, 7)}'
}

touch file.empty
echo "test" > file.empty
fallocate -l 1G random_file.bin

response=$(curl -X POST -H "Content-Type: multipart/form-data" -H "Authorization: $api_key" \
-F "package_id=test_package" \
-F "name=test_resource_to_be_large" \
-F "resource_type=Dataset" \
-F "restricted_level=public" \
-F "upload=@file.empty" \
http://localhost/api/3/action/resource_create)

url=$(get_url "$response")
resource_string=$(get_resource_string "$url")
resource_path=$(get_resource_path "$resource_string" "$storage_path")

echo "Created $url"
echo "Resource id $resource_string"
echo "Resource path $resource_path"

mv random_file.bin $resource_path

update_response=$(curl -X POST -H "Content-Type: multipart/form-data" -H "Authorization: $api_key" \
-F "package_id=test_package" \
-F "id=$resource_string" \
-F "size=1073741824" \
-F "format=bin" \
-F "description='Also adding an description here.'" \
http://localhost/api/3/action/resource_patch)

echo "$update_response"
