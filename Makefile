test:
	python -m pytest

test-all:
	python -m tox

install:
	python -m pip install --upgrade .

install-dev:
	python -m pip install --upgrade -e .[doc,test,lint]

.PHONY: docs
clean:
	rm -rdf __pycache__ .pytest_cache htmlcov .coverage build .ruff_cache .benchmarks tests/erol/.benchmarks .tox
	make -C docs clean

lint:
	isort .
	black .

cov:
	pytest --cov-report term:skip-covered --cov-report html --cov=src tests

.PHONY: docs
docs:
	make -C docs html