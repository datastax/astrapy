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

from typing import Any, Dict, Iterable, Mapping


class DataAPIDictUDT(Dict[Any, Any]):
    """
    A `dict` subclass acting as a marker for wrapping user-defined type (UDT) data.

    This object behaves entirely as a regular Python `dict`.
    Additionally, however, when encountered within a payload to be sent to the
    Data API, its ensures its contents are serialized as JSON payload in a
    format suitable for transmitting UDT data.

    Note that, depending on the Table object's `SerdesOptions`, other representations
    may be available for encoding UDT data (see the package README for more options):
    still, usage of `DataAPIDictUDT` is the one choice that works regardless of the
    serdes settings.

    When reading from Tables, correspondingly, if custom data types are enabled
    (as per default serdes options), UDT data is returned as `DataAPIDictUDT`
    dictionaries unless more specific deserialization rules are configured.
    """

    def __init__(
        self,
        initial: Mapping[str, Any] | Iterable[tuple[Any, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        if initial is None:
            initial = {}
        super().__init__(initial, **kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"
