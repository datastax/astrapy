# Project Documentation Rules (Non-Obvious Only)

## Project Structure

**Test organization:** Tests are split into three groups - "base" (general functionality, unit/integration split), "vectorize" (extensive provider testing), and "admin" (admin operations). CI only runs "base" tests.

**Test targets:** Tests can run against three different Data API targets: Astra (cloud), Local (user-supplied HCD/DSE), or DockerCompose (auto-started by tests). Each requires different environment variables from `tests/env_templates/`.

**Python version caveat:** A few integration tests (those that access the database through direct CQL connection) don't work on Python 3.13+ due to cassandra-driver dependency issues (libev/asyncore), and are therefore skipped on newer Python; but the package itself supports Python 3.13+.

## Module Organization

**Circular import pattern:** `astrapy/__init__.py` imports Database/Table at the END to avoid circular dependencies - this is intentional and documented in comments.

**Data types location:** Custom data types (DataAPIVector, DataAPIMap, DataAPISet, DataAPIDate, etc.) are in `astrapy/data_types/` - these wrap/augment/replace standard Python types with special serialization for the Data API, lift range limitations (e.g. for `datetime`) and provide the very same behaviour as the Data API needs.

**Utils organization:** `astrapy/utils/` contains critical utilities like `_UNSET` sentinel, custom `StrEnum`, Decimal encoders, and API commander. `astrapy/data/utils/` has data-specific converters.

## Testing Environment

**Environment variables mandatory:** Even unit tests require environment variables from `tests/env_templates/*.base.template` - tests will fail without them, not skip.

**Docker Compose behavior:** If `DOCKER_COMPOSE_LOCAL_DATA_API="yes"`, tests auto-start Docker Compose but do NOT clean it up automatically - manual cleanup required.

**Blockbuster fixture:** All tests use `blockbuster` fixture (autouse=True) to detect blocking I/O in async code - specific exceptions allowed in `tests/conftest.py`.

## Build System

**Package manager:** Uses `uv` for dependency management and virtual environments, not pip or poetry.

**Coverage tracking:** Unit and integration tests write separate coverage files (`.coverage.unit`, `.coverage.integration`) - combine with `make coverage`.