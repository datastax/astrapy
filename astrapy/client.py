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

from typing import Any, Dict, Optional, TYPE_CHECKING

from astrapy.admin import (
    Environment,
    build_api_endpoint,
    fetch_raw_database_info_from_id_token,
    parse_api_endpoint,
)


if TYPE_CHECKING:
    from astrapy import AsyncDatabase, Database
    from astrapy.admin import AstraDBAdmin


class DataAPIClient:
    """
    A client for using the Data API. This is the main entry point and sits
    at the top of the conceptual "client -> database -> collection" hierarchy.

    The client is created by passing a suitable Access Token. Starting from the
    client:
        - databases (Database and AsyncDatabase) are created for working with data
        - AstraDBAdmin objects can be created for admin-level work
    """

    def __init__(
        self,
        token: str,
        *,
        environment: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self.token = token
        self.environment = environment or Environment.PROD
        self._caller_name = caller_name
        self._caller_version = caller_version

    def get_database(
        self,
        id: str,
        *,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        region: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> Database:
        # lazy importing here to avoid circular dependency
        from astrapy import Database

        # need to inspect for values?
        this_db_info: Optional[Dict[str, Any]] = None
        # handle overrides. Only region is needed (namespace can stay empty)
        if region:
            _region = region
        else:
            if this_db_info is None:
                this_db_info = fetch_raw_database_info_from_id_token(
                    id=id,
                    token=self.token,
                    environment=self.environment,
                    max_time_ms=max_time_ms,
                )
            _region = this_db_info["info"]["region"]

        _token = token or self.token
        _api_endpoint = build_api_endpoint(
            environment=self.environment,
            database_id=id,
            region=_region,
        )
        return Database(
            api_endpoint=_api_endpoint,
            token=_token,
            namespace=namespace,
            caller_name=self._caller_name,
            caller_version=self._caller_version,
            api_path=api_path,
            api_version=api_version,
        )

    def get_async_database(
        self,
        id: str,
        *,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        region: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> AsyncDatabase:
        return self.get_database(
            id=id,
            token=token,
            namespace=namespace,
            region=region,
            api_path=api_path,
            api_version=api_version,
            max_time_ms=max_time_ms,
        ).to_async()

    def get_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Database:
        # lazy importing here to avoid circular dependency
        from astrapy import Database

        parsed_api_endpoint = parse_api_endpoint(api_endpoint)
        if parsed_api_endpoint is not None:
            if parsed_api_endpoint.environment != self.environment:
                raise ValueError(
                    "Environment mismatch between client and provided API endpoint."
                )
            _token = token or self.token
            return Database(
                api_endpoint=api_endpoint,
                token=_token,
                namespace=namespace,
                caller_name=self._caller_name,
                caller_version=self._caller_version,
                api_path=api_path,
                api_version=api_version,
            )
        else:
            raise ValueError("Cannot parse the provided API endpoint.")

    def get_async_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> AsyncDatabase:
        return self.get_database_by_api_endpoint(
            api_endpoint=api_endpoint,
            token=token,
            namespace=namespace,
            api_path=api_path,
            api_version=api_version,
        ).to_async()

    def get_admin(
        self,
        *,
        token: Optional[str] = None,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
    ) -> AstraDBAdmin:
        # lazy importing here to avoid circular dependency
        from astrapy.admin import AstraDBAdmin

        return AstraDBAdmin(
            token=token or self.token,
            environment=self.environment,
            caller_name=self._caller_name,
            caller_version=self._caller_version,
            dev_ops_url=dev_ops_url,
            dev_ops_api_version=dev_ops_api_version,
        )
