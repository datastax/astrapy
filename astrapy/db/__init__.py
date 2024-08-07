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

"""Core "db" subpackage, exported here to preserve import patterns."""

from __future__ import annotations

from astrapy.core.db import (
    AstraDB,
    AstraDBCollection,
    AsyncAstraDB,
    AsyncAstraDBCollection,
    logger,
)

__all__ = [
    "AstraDBCollection",
    "AsyncAstraDBCollection",
    "AstraDB",
    "AsyncAstraDB",
    "logger",
]
