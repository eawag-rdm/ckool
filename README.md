[![License](https://img.shields.io/badge/LICENSE-GPL3.0-blue)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.9%20%7C%203.11%20%7C%203.12-green)](https://www.gnu.org/licenses/gpl-3.0.en.html)

# Ckool

CKAN Tool, in short ckool! A tool for working with CKAN.

## Installation

### With Git

```shell
pip install 'ckool @ git+ssh://gitlab.switch.ch:eaw-rdm/ckool.git@main'
```

## Installation for Development

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
TEST_DATACITE_PASSWORD=YOUR-PASSWORD
TEST_DATACITE_URL=https://api.test.datacite.org
TEST_DATACITE_USER=YOUR-USERNAME
TEST_DATACITE_PREFIX=YOUR-PREFIX
TEST_DATACITE_OFFSET=0
--- inclomplete - more will follow ---
```

For running **all** tests with a fully configured `.env` file.
```shell
python -m pytest --run-slow --run-impure
```

Or you can use tox to run tests for multiple python versions. A the moment python 3.10 - 3.12 are configured.
Using pyenv to manage the different py-envs once must install these versions and the make them accessible via:
`pyenv local 3.10.x 3.11.x 3.12.x` in your project folder. Once that is done you can run tox:

```shell
python -m tox
```
If you want to configure tox, find the section: `tool.tox` in the pyproject.toml.

Some tests that are slow and/or require additional configuration are skipped by default.
To run all tests run tests with the `--run-slow` and `--run-impure` flags (`tox -- --run-impure --run-slow`).
The variables needed to run all test must be specified in the `tool.pytest.ini_options` section under `env`.


> More documentation will follow