# CHANGES.txt

## Version 0.6.2

### What's Changed

- the docstring for find_one was slightly misleading by @hemidactylus in <https://github.com/datastax/astrapy/pull/121>
- Fix bug in vector_ methods and rearrange tests by @hemidactylus in <https://github.com/datastax/astrapy/pull/123>

**Full Changelog**: <https://github.com/datastax/astrapy/compare/v0.6.1...v0.6.2>

## Version 0.6.1

### Added

- **HTTPX Support for Requests**: Introduced the integration of HTTPX to support HTTP/2 and improve concurrency.
- **Non-Vector Collection Creation**: Implemented functionality to allow creation of non-vector collections.
- **Support for the `delete_many` Operation**: Added a new AstraDBCollection method for `delete_many`, which calls the JSON API `deleteMany` endpoint to perform a multi-delete.
- **Support for the `truncate` Operation**: Added an `AstraDB.truncate()` method to truncate the data in an existing collection.
- **Enhanced Documentation**: Added comprehensive docstrings for all public methods, improving code readability and ease of use.
- **Full Support for Type Hints**: Type hints across all public functions.

### Changed

- **Beta Version Designation**: Bumped the setup.py development version designation from Alpha to Beta.
- **Endpoint URL Parameters**: Updated the handling of endpoint URL parameters to ensure future compatibility and robustness.
- **Development Requirements**: Split the requirements into a user and developer version.

### Fixed

- **Document Upsertion**: Resolved an issue with the upserting process of documents.
- **Push and Pop**: Updated the interface for `push` and `pop` calls to be more intuitive.

## Version 0.6.0

- Initial Release
