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

from typing import Any

SIMPLE_SEVEN_ROWS_OK: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": 2},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": 4},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]

SIMPLE_SEVEN_ROWS_F2: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": "boo"},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": 4},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]

SIMPLE_SEVEN_ROWS_F4: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": 2},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": "boo"},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]
