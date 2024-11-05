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

from astrapy.data.info.data_api_exceptions import DataAPIException


@dataclass
class CollectionNotFoundException(DataAPIException):
    """
    A collection is found non-existing and the requested operation
    cannot be performed.

    Attributes:
        text: a text message about the exception.
        keyspace: the keyspace where the collection was supposed to be.
        collection_name: the name of the expected collection.
    """

    text: str
    keyspace: str
    collection_name: str

    def __init__(
        self,
        text: str,
        *,
        keyspace: str,
        collection_name: str,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.keyspace = keyspace
        self.collection_name = collection_name
