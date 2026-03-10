# Project Architecture Rules (Non-Obvious Only)

## Core Architecture

**Dual API pattern:** Project provides both sync and async versions of all major classes (Collection/AsyncCollection, Database/AsyncDatabase, Table/AsyncTable) - they share similar interfaces but are separate implementations (i.e. no thread-wrapping for faux async, ever).

**Circular dependency resolution:** `astrapy/__init__.py` imports Database/Table at the END of the module to break circular dependencies between client, database, and collection/table classes.

## Data Flow

**Unset vs None distinction:** The `_UNSET` sentinel from `astrapy.utils.unset` is used throughout to distinguish "parameter not provided" from "explicitly set to None" - critical for API calls where omitting a parameter has different semantics than passing None.

**Decimal handling:** JSON de/serialization for API responses/payloads is customized in `astrapy/utils/api_commander.py` to respect the Decimal serialization down to all its significant digits. See the "decimal-aware" parse/encode methods of the `APICommander` class. This interacts non-trivially with the actual data type the user sees when reading (e.g. floats), which is configured through the "SerdesOptions".

**Type conversion layers:** `astrapy/data/utils/table_converters.py` contains complex type conversion logic for Table operations - handles Python types to/from Data API representations including special handling for vectors, maps, sets, UDTs, dates, times, timestamps.

**Return type classes:** Classes that represent (part of) Data API responses have the same shape as the response itself. Their standardized `_from_dict` method MUST raise a warning when it encounters unexpected additional fields, but still work (there are utilities for that). Also, generally a side field is also stored with the original input dict (`raw_response`, `raw_input` or similar).

## Testing Architecture

**Three test groups:** Tests organized as "base" (CI-tested general functionality), "vectorize" (manual provider testing), and "admin" (manual admin operations). Only "base" runs in CI.

**Environment-dependent behavior:** Tests adapt to three different targets (Astra/Local/DockerCompose) based on environment variables - different features available on each.

**Blockbuster integration:** All tests use `blockbuster` fixture (autouse=True) to detect blocking I/O in async code - prevents accidental blocking calls in async contexts.

## Non-Standard Patterns

**StrEnum with lookup:** Custom `StrEnum` class in `astrapy/utils/str_enum.py` has special `_name_lookup` method for case-insensitive enum lookups - not available in standard library Enum.

**Docker Compose lifecycle:** If `DOCKER_COMPOSE_LOCAL_DATA_API="yes"`, tests auto-start Docker Compose via `tests/preprocess_env.py` but do NOT clean it up - intentional for debugging.

**Python version caveat:** A few integration tests (those that access the database through direct CQL connection) don't work on Python 3.13+ due to cassandra-driver dependency issues (libev/asyncore), and are therefore skipped on newer Python; but the package itself supports Python 3.13+.
