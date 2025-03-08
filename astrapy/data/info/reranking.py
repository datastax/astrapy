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

from dataclasses import dataclass
from typing import Any


@dataclass
class RerankingServiceOptions:
    """
    TODO DOCSTRING TODO
    """

    provider: str | None
    model_name: str | None
    authentication: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "provider": self.provider,
                "modelName": self.model_name,
                "authentication": self.authentication,
                "parameters": self.parameters,
            }.items()
            if v is not None
        }

    @staticmethod
    def _from_dict(
        raw_dict: dict[str, Any] | None,
    ) -> RerankingServiceOptions | None:
        """
        Create an instance of RerankingServiceOptions from a dictionary
        such as one from the Data API.
        """

        if raw_dict is not None:
            return RerankingServiceOptions(
                provider=raw_dict.get("provider"),
                model_name=raw_dict.get("modelName"),
                authentication=raw_dict.get("authentication"),
                parameters=raw_dict.get("parameters"),
            )
        else:
            return None

    @staticmethod
    def coerce(
        raw_input: RerankingServiceOptions | dict[str, Any] | None,
    ) -> RerankingServiceOptions | None:
        if isinstance(raw_input, RerankingServiceOptions):
            return raw_input
        else:
            return RerankingServiceOptions._from_dict(raw_input)
