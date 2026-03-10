# Project Advance Coding Rules (Non-Obvious Only)

**Copyright notice MANDATORY:** All Python files, including empty ones (`__init__.py`), must start with the same copyright notice (as can be seen by reading any of them).

## Import Requirements

**Future annotations MANDATORY:** Every Python file MUST have `from __future__ import annotations` as line 15 (first import after copyright header). This is enforced project-wide for Python 3.8+ compatibility.

## Custom Utilities (Must Use)

**Unset sentinel:** In most cases, use `_UNSET` from `astrapy.utils.unset` instead of `None` for optional parameters. This singleton distinguishes "not provided" from "explicitly None" - critical for API parameter handling where the semantics is different.

**Decimal handling:** JSON de/serialization for API responses/payloads is customized in `astrapy/utils/api_commander.py` to respect the Decimal serialization down to all its significant digits. See the "decimal-aware" parse/encode methods of the `APICommander` class. This interacts non-trivially with the actual data type the user sees when reading (e.g. floats), which is configured through the "SerdesOptions".

**String enums:** Use custom `StrEnum` from `astrapy/utils/str_enum.py` instead of standard library Enum. It has special `_name_lookup` method for case-insensitive lookups.

## Type Hints (Strict)

**Mypy strict mode:** All functions require complete type hints. No implicit reexports, no untyped calls, no untyped decorators. This is enforced by pyproject.toml.

**Forward references:** Use `from __future__ import annotations` to enable forward references without quotes.

## Testing Patterns

**Async/sync naming:** Test files MUST end with `_async` or `_sync` suffix. This is a strict organizational convention, not optional.

**Blockbuster fixture:** All tests automatically use `blockbuster` fixture (autouse=True) to detect blocking I/O in async code. Specific exceptions are allowed in `tests/conftest.py` lines 86-89.

**Environment variables required:** Even unit tests require environment variables from `tests/env_templates/*.base.template`. Tests will fail without them.

## Non-Standard Patterns

**Circular import workaround:** `astrapy/__init__.py` imports Database/Table at the END of the module (lines 62-63) to avoid circular import issues. Don't move these imports.

**Docker Compose cleanup:** If `DOCKER_COMPOSE_LOCAL_DATA_API="yes"`, tests auto-start Docker Compose via `tests/preprocess_env.py` but do NOT clean it up automatically.

## MCP and Browser Access

This mode has access to MCP tools and browser capabilities for enhanced development workflows.