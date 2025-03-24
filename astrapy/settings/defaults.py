# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import datetime

# whether to go the extra mile and ensure the "decimal escape trick" is
# not colliding with legitimate user-provided (extremely rare) strings.
# A system setting, off at this time
CHECK_DECIMAL_ESCAPING_CONSISTENCY = False

# Environment names for management internal to astrapy
DATA_API_ENVIRONMENT_PROD = "prod"
DATA_API_ENVIRONMENT_DEV = "dev"
DATA_API_ENVIRONMENT_TEST = "test"
DATA_API_ENVIRONMENT_DSE = "dse"
DATA_API_ENVIRONMENT_HCD = "hcd"
DATA_API_ENVIRONMENT_CASSANDRA = "cassandra"
DATA_API_ENVIRONMENT_OTHER = "other"

# Defaults/settings for Database management
DEFAULT_ASTRA_DB_KEYSPACE = "default_keyspace"
API_ENDPOINT_TEMPLATE_ENV_MAP = {
    DATA_API_ENVIRONMENT_PROD: "https://{database_id}-{region}.apps.astra.datastax.com",
    DATA_API_ENVIRONMENT_DEV: "https://{database_id}-{region}.apps.astra-dev.datastax.com",
    DATA_API_ENVIRONMENT_TEST: "https://{database_id}-{region}.apps.astra-test.datastax.com",
}
API_PATH_ENV_MAP = {
    DATA_API_ENVIRONMENT_PROD: "/api/json",
    DATA_API_ENVIRONMENT_DEV: "/api/json",
    DATA_API_ENVIRONMENT_TEST: "/api/json",
    #
    DATA_API_ENVIRONMENT_DSE: "",
    DATA_API_ENVIRONMENT_HCD: "",
    DATA_API_ENVIRONMENT_CASSANDRA: "",
    DATA_API_ENVIRONMENT_OTHER: "",
}
API_VERSION_ENV_MAP = {
    DATA_API_ENVIRONMENT_PROD: "/v1",
    DATA_API_ENVIRONMENT_DEV: "/v1",
    DATA_API_ENVIRONMENT_TEST: "/v1",
    #
    DATA_API_ENVIRONMENT_DSE: "v1",
    DATA_API_ENVIRONMENT_HCD: "v1",
    DATA_API_ENVIRONMENT_CASSANDRA: "v1",
    DATA_API_ENVIRONMENT_OTHER: "v1",
}

# Defaults/settings for Data API requests
DEFAULT_USE_DECIMALS_IN_COLLECTIONS = False
DEFAULT_BINARY_ENCODE_VECTORS = True
DEFAULT_CUSTOM_DATATYPES_IN_READING = True
DEFAULT_UNROLL_ITERABLES_TO_LISTS = False
DEFAULT_ENCODE_MAPS_AS_LISTS_IN_TABLES = "NEVER"  # later coerced as MapEncodingMode

DEFAULT_ACCEPT_NAIVE_DATETIMES = False
DEFAULT_DATETIME_TZINFO = datetime.timezone.utc

DEFAULT_INSERT_MANY_CHUNK_SIZE = 50
DEFAULT_INSERT_MANY_CONCURRENCY = 20
DEFAULT_REQUEST_TIMEOUT_MS = 10000
DEFAULT_GENERAL_METHOD_TIMEOUT_MS = 30000
DEFAULT_COLLECTION_ADMIN_TIMEOUT_MS = 60000
DEFAULT_TABLE_ADMIN_TIMEOUT_MS = 30000
DEFAULT_DATA_API_AUTH_HEADER = "Token"
EMBEDDING_HEADER_AWS_ACCESS_ID = "X-Embedding-Access-Id"
EMBEDDING_HEADER_AWS_SECRET_ID = "X-Embedding-Secret-Id"
EMBEDDING_HEADER_API_KEY = "X-Embedding-Api-Key"
RERANKING_HEADER_API_KEY = "Reranking-Api-Key"

# Defaults/settings for DevOps API requests and admin operations
DEFAULT_DEV_OPS_AUTH_HEADER = "Authorization"
DEFAULT_DEV_OPS_AUTH_PREFIX = "Bearer "
DEV_OPS_KEYSPACE_POLL_INTERVAL_S = 2
DEV_OPS_DATABASE_POLL_INTERVAL_S = 15
DEFAULT_DATABASE_ADMIN_TIMEOUT_MS = 600000
DEFAULT_KEYSPACE_ADMIN_TIMEOUT_MS = 30000
DEV_OPS_DATABASE_STATUS_MAINTENANCE = "MAINTENANCE"
DEV_OPS_DATABASE_STATUS_ACTIVE = "ACTIVE"
DEV_OPS_DATABASE_STATUS_PENDING = "PENDING"
DEV_OPS_DATABASE_STATUS_INITIALIZING = "INITIALIZING"
DEV_OPS_DATABASE_STATUS_ERROR = "ERROR"
DEV_OPS_DATABASE_STATUS_TERMINATING = "TERMINATING"
DEV_OPS_URL_ENV_MAP = {
    DATA_API_ENVIRONMENT_PROD: "https://api.astra.datastax.com",
    DATA_API_ENVIRONMENT_DEV: "https://api.dev.cloud.datastax.com",
    DATA_API_ENVIRONMENT_TEST: "https://api.test.cloud.datastax.com",
}
DEV_OPS_VERSION_ENV_MAP = {
    DATA_API_ENVIRONMENT_PROD: "v2",
    DATA_API_ENVIRONMENT_DEV: "v2",
    DATA_API_ENVIRONMENT_TEST: "v2",
}
DEV_OPS_RESPONSE_HTTP_ACCEPTED = 202
DEV_OPS_RESPONSE_HTTP_CREATED = 201
DEV_OPS_DEFAULT_DATABASES_PAGE_SIZE = 50

# Settings for redacting secrets in string representations and logging
SECRETS_REDACT_ENDING = "..."
SECRETS_REDACT_CHAR = "*"
SECRETS_REDACT_ENDING_LENGTH = 3
FIXED_SECRET_PLACEHOLDER = "***"
DEFAULT_REDACTED_HEADER_NAMES = {
    DEFAULT_DATA_API_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    EMBEDDING_HEADER_AWS_ACCESS_ID,
    EMBEDDING_HEADER_AWS_SECRET_ID,
    EMBEDDING_HEADER_API_KEY,
    RERANKING_HEADER_API_KEY,
}
