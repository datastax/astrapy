# CHANGES.txt

## Version 0.7.1

###  What's Changed

* Align sync and async behavior regarding exceptions in chunked_insert_ many by @cbornet in https://github.com/datastax/astrapy/pull/164
* Align make_request and amake_request by @cbornet in https://github.com/datastax/astrapy/pull/165
* Adjust tests to the ordered=False API default by @hemidactylus in https://github.com/datastax/astrapy/pull/169
* Feature/#166 slick truncate by @hemidactylus in https://github.com/datastax/astrapy/pull/172
* Support for $date, generalized pre- and post-processing, comprehensive management of vector coercion by @hemidactylus in https://github.com/datastax/astrapy/pull/167
* rework upsert flow to handle all errors from API by @hemidactylus in https://github.com/datastax/astrapy/pull/170

## Version 0.7.0

### What's Changed

- the authentication header for the JSON Api is changed from `X-Cassandra-Token` to `Token`, in line with changes in the API by @hemidactylus in <https://github.com/datastax/astrapy/pull/73>
- the parameter name for pagination is changed from `nextPagingState` to `nextPageState`, in line with changes in the API by @hemidactylus in <https://github.com/datastax/astrapy/pull/72>
- Raise HTTP errors by @bjchambers in https://github.com/datastax/astrapy/pull/125
- AstraDBCollection count_documents method by @hemidactylus in https://github.com/datastax/astrapy/pull/131
- Fix #130: Use path in the _post calls by @erichare in https://github.com/datastax/astrapy/pull/132
- fix OPS_API_RESPONSE union type to include lists by @hemidactylus in https://github.com/datastax/astrapy/pull/137
- Add async API by @cbornet in https://github.com/datastax/astrapy/pull/146
- Add a trace logging level and use for payload data by @erichare in https://github.com/datastax/astrapy/pull/148
- Use poetry to manage the project by @cbornet in https://github.com/datastax/astrapy/pull/153
- Add optional pre-fetching of paginate results by @cbornet in https://github.com/datastax/astrapy/pull/154
- Ensure that data gets coerced to JSON-serializable floats by @erichare in https://github.com/datastax/astrapy/pull/144
- a test collecting all non-equality operators in the filters to find by @hemidactylus in https://github.com/datastax/astrapy/pull/162
- Batched versions of insert / upsert by @erichare in https://github.com/datastax/astrapy/pull/134
- Fix #126 - improve handling of API errors by @erichare in https://github.com/datastax/astrapy/pull/128
- Add async insert_many_chunked and upsert_many by @cbornet in https://github.com/datastax/astrapy/pull/157

## New Contributors

* @bjchambers made their first contribution in https://github.com/datastax/astrapy/pull/125
* @cbornet made their first contribution in https://github.com/datastax/astrapy/pull/146

**Full Changelog**: https://github.com/datastax/astrapy/compare/v0.6.2...v0.7.0

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
