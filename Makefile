SHELL := /bin/bash

.PHONY: all venv format format-fix format-tests format-src test-integration test build help

all: help

FMT_FLAGS ?= --check

venv:
	uv venv
	uv sync --dev

format: format-src format-tests

format-tests:
	uv run ruff check tests
	uv run ruff format tests $(FMT_FLAGS)
	uv run mypy tests

format-src:
	uv run ruff check astrapy
	uv run ruff format astrapy $(FMT_FLAGS)
	uv run mypy astrapy

format-fix: format-fix-src format-fix-tests

format-fix-src: FMT_FLAGS=
format-fix-src: format-src

format-fix-tests: FMT_FLAGS=
format-fix-tests: format-tests

test-integration:
	uv run pytest tests/base -vv

test:
	uv run pytest tests/base/unit -vv

docker-test-integration:
	DOCKER_COMPOSE_LOCAL_DATA_API="yes" uv run pytest tests/base -vv

build:
	rm -f dist/astrapy*
	uv build

help:
	@echo "======================================================================"
	@echo "AstraPy make command             purpose"
	@echo "----------------------------------------------------------------------"
	@echo "venv                             create a virtual env (needs uv)"
	@echo "format                           full lint and format checks"
	@echo "  format-src                       limited to source"
	@echo "  format-tests                     limited to tests"
	@echo "  format-fix                       fixing imports and style"
	@echo "    format-fix-src                   limited to source"
	@echo "    format-fix-tests                 limited to tests"
	@echo "test                   					run unit tests"
	@echo "test-integration              		run integration tests"
	@echo "docker-test-integration       		run int.tests on dockerized local"
	@echo "build                            build package ready for PyPI"
	@echo "======================================================================"
