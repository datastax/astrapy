# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Build/Test Commands

**Run single test file:**
```bash
uv run pytest tests/base/unit/test_exceptions.py -v
```

**Run single test by name pattern:**
```bash
uv run pytest tests/base/integration -k test_collection_count_documents_sync -v
```

**Run tests in specific directory:**
```bash
uv run pytest tests/base/unit -v          # Unit tests only
uv run pytest tests/base/integration -v   # Integration tests only
```

**Environment setup required:** Tests require environment variables from `tests/env_templates/*.base.template` even for unit tests. Integration tests also need `env.vectorize-minimal.template` variables.

**Regularly-run vs. manual tests:** Tests in `tests/base` are part of the "always-run" process, whereas admin and vectorize tests (long and cumbersome) are manually run, outside of Github CI/CD, only when changes call for it or new API versions are deployed.

## Code Style (Non-Obvious)

**Import order:** Always use `from __future__ import annotations` as the FIRST import after copyright header (line 15 in all files).

**Unset sentinel:** Use `_UNSET` from `astrapy.utils.unset` instead of `None` for optional parameters that need to distinguish "not provided" from "explicitly None".

**Type hints:** Project uses Python 3.8+ with `from __future__ import annotations` for forward references. All functions must have complete type hints (enforced by mypy strict mode).

**Decimal handling:** JSON de/serialization for API responses/payloads is customized in `astrapy/utils/api_commander.py` to respect the Decimal serialization down to all its significant digits. See the "decimal-aware" parse/encode methods of the `APICommander` class. This interacts non-trivially with the actual data type the user sees when reading (e.g. floats), which is configured through the "SerdesOptions".

**StrEnum pattern:** Custom `StrEnum` class in `astrapy/utils/str_enum.py` with special `_name_lookup` - use this instead of standard Enum for string enums.

**Test environment detection:** Tests use `tests/preprocess_env.py` which auto-starts Docker Compose if `DOCKER_COMPOSE_LOCAL_DATA_API="yes"` - this is NOT cleaned up automatically.

**Python 3.13+ limitation:** Integration tests don't work (and are hence skipped) on Python 3.13+ due to cassandra-driver dependency issues (libev/asyncore), but the package itself supports 3.13.

## Formatting

**Linter:** Uses `ruff` with specific rules as per pyproject.toml.

**Auto-fix:** Run `make format-fix` to auto-fix imports and style issues before committing.

**Type checking:** Strict mypy configuration - all functions require type hints, no implicit reexports, no untyped calls/decorators. Refer to `make check` and settings in pyproject.toml for the whole story.

**Checks before opening a PR:** Manually check that `make format` passes before submitting a PR. It will check style, linter and typing in one go for the library and the tests.

## Testing Gotchas

**Blockbuster fixture:** All tests use `blockbuster` fixture (autouse=True) to detect blocking I/O - allows specific exceptions in `tests/conftest.py`.

**Coverage files:** Unit and integration tests write separate coverage files (`.coverage.unit`, `.coverage.integration`) - combine with `make coverage`.

**Test naming:** Async tests end with `_async`, sync tests end with `_sync` - this is a strict convention for test file organization.