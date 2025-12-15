SHELL := /bin/bash

.PHONY: all venv format format-fix format-tests format-src test-integration test build help

all: help

FMT_FLAGS ?= --check
VENV ?= false

ifeq ($(VENV), true)
  VENV_FLAGS := --active
else
  VENV_FLAGS :=
endif

venv:
	uv venv
	uv sync --dev

format: format-src format-tests

format-tests:
	uv run $(VENV_FLAGS) ruff check tests
	uv run $(VENV_FLAGS) ruff format tests $(FMT_FLAGS)
	uv run $(VENV_FLAGS) mypy tests

format-src:
	uv run $(VENV_FLAGS) ruff check astrapy
	uv run $(VENV_FLAGS) ruff format astrapy $(FMT_FLAGS)
	uv run $(VENV_FLAGS) mypy astrapy

format-fix: format-fix-src format-fix-tests

format-fix-src: FMT_FLAGS=
format-fix-src: format-src

format-fix-tests: FMT_FLAGS=
format-fix-tests: format-tests

test-integration:
	COVERAGE_FILE=".coverage.integration" uv run $(VENV_FLAGS) pytest --cov=astrapy/ tests/base/integration -vv

test:
	COVERAGE_FILE=".coverage.unit" uv run $(VENV_FLAGS) pytest --cov=astrapy/ tests/base/unit -vv

docker-test-integration:
	DOCKER_COMPOSE_LOCAL_DATA_API="yes" uv run pytest --cov=astrapy/ tests/base/integration -vv

build:
	rm -f dist/astrapy*
	uv build

coverage:
	rm htmlcov -rf
	uv run coverage combine $(ls .coverage.unit .coverage.integration 2>/dev/null)
	uv run coverage html
	echo "OPEN file://${PWD}/htmlcov/index.html"

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
	@echo "test                                     run unit tests"
	@echo "test-integration                     run integration tests"
	@echo "docker-test-integration              run int.tests on dockerized local"
	@echo "coverage                         HTML coverage map from last test"
	@echo "build                            build package ready for PyPI"
	@echo "======================================================================"
