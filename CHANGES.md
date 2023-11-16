# CHANGES.txt

## Version 0.6.1

### Added

- **HTTPX Support for Concurrency**: Introduced the integration of HTTPX to enhance concurrency capabilities.
- **Non-Vector Collection Creation**: Implemented functionality to allow creation of non-vector collections, broadening the scope of collection types supported.
- **Support for the `delete_many` Operation**: Added a new AstraDBCollection method for `delete_many`, which calls the JSON API `deleteMany` endpoint to perform a multi-delete.
- **Enhanced Documentation**: Added comprehensive docstrings for all public methods, improving code readability and ease of use.

### Changed

- **Beta Version Designation**: Bumped the setup.y development version designation from Alpha to Beta
- **Endpoint URL Parameters**: Updated the handling of endpoint URL parameters to ensure future compatibility and robustness.

### Fixed

- **Document Upsertion**: Resolved an issue with the upserting process of documents, enhancing data consistency and reliability.
- **Push and Pop**: Updated the interface for `push` and `pop` calls to be more intuitive.

### Removed

- **Python-dotenv Dependency**: Removed the python-dotenv package from the setup, optimizing dependency management.

## Version 0.6.0

- Initial Release
