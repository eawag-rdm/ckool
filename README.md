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
TEST_DATACITE_PASSWORD=...
TEST_DATACITE_URL=https://api.test.datacite.org
TEST_DATACITE_USER=...

TEST_DATACITE_PREFIX=...
TEST_DATACITE_OFFSET=0

# SecureInterface
SECURE_INTERFACE_HOST=...
SECURE_INTERFACE_PORT=...
SECURE_INTERFACE_USERNAME=ckan
SECURE_INTERFACE_PASSWORD=
SECURE_INTERFACE_SSH_KEY=...
SECURE_INTERFACE_PASSPHRASE=

CKAN_STORAGE_PATH=...

CKAN_URL=https://localhost:8443
CKAN_TOKEN=...
CKAN_TEST_PACKAGE_NAME=test_package
CKAN_TEST_ORGANIZATION_NAME=test_organization
CKAN_TEST_RESOURCE_NAME = test_resource_link
CKAN_TEST_GROUP_NAME=test_group
```

For running **all** tests with a fully configured `.env` file.
```shell
python -m pytest --run-slow --run-impure
```

Or you can use tox to run tests for multiple python versions. At the moment python 3.11 and 3.12 are configured.
Using pyenv to manage the different py-envs once must install these versions and the make them accessible via:
`pyenv local 3.11.x 3.12.x` in your project folder. Once that is done you can run tox:

```shell
python -m tox
```
If you want to configure tox, find the section: `tool.tox` in the pyproject.toml.

Some tests that are slow and/or require additional configuration are skipped by default.
To run all tests run tests with the `--run-slow` and `--run-impure` flags (`tox -- --run-impure --run-slow`).
The variables needed to run all test must be specified in the `tool.pytest.ini_options` section under `env`.


## Usage
You can use the tool via its CLI. 

```shell
ckool --help
```

### Publishing example

```shell

```

## Additional notes
- For uploading resource via API a user that does have permissions to write in the ckan datastorage location must be present on the system. (ERIC Open - ckan user created on system)
- 