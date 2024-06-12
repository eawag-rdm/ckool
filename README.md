[![License](https://img.shields.io/badge/LICENSE-GPL3.0-blue)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-green)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![Tests](https://gitlab.switch.ch/eaw-rdm/ckool/badges/main/pipeline.svg)](https://gitlab.switch.ch/eaw-rdm/ckool/-/commits/main)

# Ckool

CKAN Tool, in short ckool! A tool for working with CKAN. 

> **Current limitations**:
> - the machine running the publishing process needs enough free storage to hold the entire package it wants to publish 
> - no complex parallel workflows at the moment
> - implement better tracking for uploading and publishing, so that already uploaded/published resources will not be tried again
> - expose more functions to CLI? Eg.: access to datastore, datacite, dora
> - Add function documentation for API - create readthedocs

TODO's
- cleanup `conftest.py`
- cleanup the way config is passed to functions,
  - more consistency is needed for different level function


## Installation

### With Git

```shell
pip install 'ckool @ git+ssh://gitlab.switch.ch:eaw-rdm/ckool.git@main'
```

## Installation for Development (easy updates via git pull)

Clone this repo:
```shell
git clone https://gitlab.switch.ch/eaw-rdm/ckool.git
```

Install via:
```shell
pip install -e .[dev]
```

## Testing

You can run test in your local venv via:

```shell
python -m pytest  
```

For some tests environment variables are required. Create an .env file called `.env` in the directory that contains the `conftest.py` file.
The file should contain:
```env
DATACITE_PASSWORD=...
DATACITE_URL=https://api.test.datacite.org
DATACITE_USER=...
DATACITE_PREFIX=...
DATACITE_OFFSET=...

CKAN_PACKAGE_NAME=test_package
CKAN_ORGANIZATION_NAME=test_organization
CKAN_RESOURCE_NAME = test_resource_link
CKAN_GROUP_NAME=test_group

# SecureInterface
INTERNAL_SECURE_INTERFACE_HOST=...
INTERNAL_SECURE_INTERFACE_PORT=...
INTERNAL_SECURE_INTERFACE_USERNAME=...
INTERNAL_SECURE_INTERFACE_PASSWORD=
INTERNAL_SECURE_INTERFACE_SSH_KEY=...
INTERNAL_SECURE_INTERFACE_PASSPHRASE=

INTERNAL_CKAN_STORAGE_PATH=/var/lib/ckan
INTERNAL_CKAN_URL=https://localhost:8443
INTERNAL_CKAN_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJTVlJJUFZzcS1XcGFhVkMwR28ZTUdCdjJwd3lFalZwdTVrcnV4ZzNFMHhqdVF2NDVINEVqTUtpOU93NGtQSVpBbmJfMXJSY3dFbk5rSWIyVCIsImlhdCI6MTcwNjcwMjQ4N30.l6yRKh-MeL-sBZYGbRpuZRaXKxmc3i4db0jtk5cxAZQ

# SecureInterface
OPEN_SECURE_INTERFACE_HOST=...
OPEN_SECURE_INTERFACE_PORT=...
OPEN_SECURE_INTERFACE_USERNAME=...
OPEN_SECURE_INTERFACE_PASSWORD=
OPEN_SECURE_INTERFACE_SSH_KEY=...
OPEN_SECURE_INTERFACE_PASSPHRASE=

OPEN_CKAN_STORAGE_PATH=/var/lib/ckan
OPEN_CKAN_URL=https://localhost:8444
OPEN_CKAN_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJ6X0YwRkpOcGNwMkpWOHhPLTJYeEk3a1ZpQXRORUNqVHhjUHR42k4zcjRkTXVSVDBjTXRIUnhieTAxMkdHYmY3VlNNRmdlcFhuT0VZNWRaRyIsImlhdCI6MTcxMTU0MDgzMH0.LAJkLGlJclp9sAG-jgY5qd9EuQF9ApnVIOLOc28sHvw

```

For running **all** tests with a fully configured `.env` file.
```shell
python -m pytest --run-slow --run-impure --run-dora --run-open
```

Or you can use tox to run tests for multiple python versions. At the moment python 3.11 and 3.12 are configured.
Using pyenv to manage the different py-envs once must install these versions and the make them accessible via:
`pyenv local 3.11.x 3.12.x` in your project folder. Once that is done you can run tox:

```shell
python -m tox
```
If you want to configure tox, find the section: `tool.tox` in the pyproject.toml.

Some tests that are slow and/or require additional configuration are skipped by default.
To run all tests run tests with the flags `--run-slow`, `--run-impure`, `--run-dora` and `--run-open` flags (`tox -- --run-impure --run-slow --run-dora --run-open`).
The variables needed to run all test must be specified in the `tool.pytest.ini_options` section under `env`.


## Usage
You can use the tool via its CLI. 

### Additional Setup

For taking full advantage of `ckool`'s functionalities, you need to have an API key (or token) ready to be used and for large file uploads, that will use **ssh** you will need an ssh key on the server. 
Make sure the key is stored in the right user account, which must have write permissions in the ckan resource store. 

```shell
ckool --help
```

### Examples

The `get` interface allows you to retrieve useful information from any configured ckan instance:
```shell
# get the metadata of a package
ckool get --no-verify -ci eric_staging metadata data-for-degradation-dispersal-cycles-on-polysaccharides

# display local path of resource
ckool get --no-verify -ci eric_open_staging local-path greifensee-abiotic-data-2018-june-2023 ReadMe.txt

# get all metadata from ckan instance
ckool get --no-verify -ci eric_open_staging all_metadata
```

Via `publish` packages can be moved from one ckan instance to another.
```shell
# with official CA certificate
ckool -v publish -ci eric_staging package -cit eric_open_staging this-is-a-ckool-test-package -cdi -er large-file-3.img --hash-source-resources

# without official ca certificate
ckool publish --no-verify -ci eric_staging package -cit eric_open_staging --check-data-integrity --exclude-resources abc.bin --force-scp --keep-resources test-package-123
```

Via `patch` data within packages can be changed:
```shell
# update a resource hash
ckool -v patch -ci eric_open_staging resource_hash this-is-a-ckool-test-package large-file-2.img
```

## Additional notes
- For uploading resource via API a user that does have permissions to write in the ckan datastorage location must be present on the system. (ERIC Open - ckan user created on system)
- 