SHELL := /bin/bash

.PHONY: all format format-fix format-tests format-src test-idiomatic test-idiomatic-unit test-idiomatic-integration build help

all: help

FMT_FLAGS ?= --check

format: format-src format-tests

format-tests:
	poetry run ruff check tests
	poetry run isort tests $(FMT_FLAGS) --profile black
	poetry run black tests $(FMT_FLAGS)
	poetry run mypy tests

format-src:
	poetry run ruff check astrapy
	poetry run isort astrapy $(FMT_FLAGS) --profile black
	poetry run black astrapy $(FMT_FLAGS)
	poetry run mypy astrapy

format-fix: format-fix-src format-fix-tests

format-fix-src: FMT_FLAGS=
format-fix-src: format-src

format-fix-tests: FMT_FLAGS=
format-fix-tests: format-tests

test-idiomatic:
	poetry run pytest tests/idiomatic -vv

test-idiomatic-unit:
	poetry run pytest tests/idiomatic/unit -vv

test-idiomatic-integration:
	poetry run pytest tests/idiomatic/integration -vv

docker-test-idiomatic:
	DOCKER_COMPOSE_LOCAL_DATA_API="yes" poetry run pytest tests/idiomatic -vv

docker-test-idiomatic-unit:
	DOCKER_COMPOSE_LOCAL_DATA_API="yes" poetry run pytest tests/idiomatic/unit -vv

docker-test-idiomatic-integration:
	DOCKER_COMPOSE_LOCAL_DATA_API="yes" poetry run pytest tests/idiomatic/integration -vv

build:
	rm -f dist/astrapy*
	poetry build

help:
	@echo "======================================================================"
	@echo "AstraPy make command             purpose"
	@echo "----------------------------------------------------------------------"
	@echo "format                           full lint and format checks"
	@echo "  format-src                       limited to source"
	@echo "  format-tests                     limited to tests"
	@echo "  format-fix                       fixing imports and style"
	@echo "    format-fix-src                   limited to source"
	@echo "    format-fix-tests                 limited to tests"
	@echo "test-idiomatic                   run idiomatic tests"
	@echo "  test-idiomatic-unit              unit only"
	@echo "  test-idiomatic-integration       integration only"
	@echo "docker-test-idiomatic            same, on docker container"
	@echo "  docker-test-idiomatic-unit           same"
	@echo "  docker-test-idiomatic-integration    same"
	@echo "build                            build package ready for PyPI"
	@echo "======================================================================"
