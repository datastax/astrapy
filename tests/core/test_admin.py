import pytest

from astrapy.admin import parse_generic_api_url


@pytest.mark.describe("should parse an http endpoint")
def test_parse_http_endpoint() -> None:
    raw_api_endpoint = "http://127.0.0.1"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://127.0.0.1"


@pytest.mark.describe("should parse an https endpoint")
def test_parse_https_endpoint() -> None:
    raw_api_endpoint = "https://127.0.0.1"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "https://127.0.0.1"


@pytest.mark.describe("should parse an http endpoint with port")
def test_parse_http_endpoint_with_port() -> None:
    raw_api_endpoint = "http://127.0.0.1:8080"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://127.0.0.1:8080"


@pytest.mark.describe("should parse an ip-based endpoint")
def test_parse_endpoint_ip() -> None:
    raw_api_endpoint = "http://127.0.0.1"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://127.0.0.1"


@pytest.mark.describe("should strip trailing slash")
def test_strip_slash() -> None:
    raw_api_endpoint = "http://127.0.0.1/"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://127.0.0.1"


@pytest.mark.describe("should parse a domain-based endpoint without hyphen")
def test_parse_endpoint_domain_without_hyphen() -> None:
    raw_api_endpoint = "http://my.domain"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://my.domain"


@pytest.mark.describe("should parse a domain-based endpoint with hyphen")
def test_parse_endpoint_domain_with_hyphen() -> None:
    raw_api_endpoint = "http://my-example.domain"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint == "http://my-example.domain"


@pytest.mark.describe("should fail to parse an invalid endpoint")
def test_fail_parse_invalid_endpoint() -> None:
    raw_api_endpoint = "http://%$foo/"
    parsed_api_endpoint = parse_generic_api_url(raw_api_endpoint)

    assert parsed_api_endpoint is None
