[![License](https://img.shields.io/badge/LICENSE-GPL3.0-blue)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![Python](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-green)](https://www.gnu.org/licenses/gpl-3.0.en.html)

# Erol

ERIC Tool, in short erol! A tool for working with ERIC, the Eawag Research Data Institutional Repository.

## Installation



### With Git

```shell
pip install 'erol @ git+https://sissource.ethz.ch/sispub/datapool2/poolkit@main'
```

## Installation for Development

Clone this repo:
```shell
git clone https://gitlab.switch.ch/eaw-rdm/erol.git
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

Or you can use tox to run tests for multiple python versions. A the moment python 3.8 - 3.11 are configured.
Using pyenv to manage the different py-envs once must install these versions and the make them accessible via:
`pyenv local 3.8.x 3.9.x 3.10.x 3.11.x` in your project folder. Once that is done you can run tox:

```shell
python -m tox
```
If you want to configure tox, find the section: `tool.tox` in the pyproject.toml.

Some tests that are slow and/or require additional configuration are skipped by default.
To run all tests run tests with the `--runall` flag (must be configured in the pyproject.toml for tox).
The variables needed to run all test must be specified in the `tool.pytest.ini_options` section under `env`.


> More documentation will follow