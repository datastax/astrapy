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

from typing import Sequence

from astrapy import __version__
from astrapy.constants import CallerType


def detect_astrapy_user_agent() -> CallerType:
    package_name = __name__.split(".")[0]
    return (package_name, __version__)


def compose_user_agent_string(
    caller_name: str | None, caller_version: str | None
) -> str | None:
    if caller_name:
        if caller_version:
            return f"{caller_name}/{caller_version}"
        else:
            return f"{caller_name}"
    else:
        return None


def compose_full_user_agent(callers: Sequence[CallerType]) -> str | None:
    user_agent_strings = [
        ua_string
        for ua_string in (
            compose_user_agent_string(caller[0], caller[1]) for caller in callers
        )
        if ua_string
    ]
    if user_agent_strings:
        return " ".join(user_agent_strings)
    else:
        return None
