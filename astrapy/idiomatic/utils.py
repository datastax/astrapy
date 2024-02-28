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
from functools import wraps
from typing import Any, Callable

DEFAULT_NOT_SUPPORTED_MESSAGE = "Operation not supported."
DEFAULT_NOT_SUPPORTED_PARAMETER_MESSAGE_TEMPLATE = "Parameter `{parameter_name}` not supported for method `{method_name}` of class `{class_name}`."


def unsupported(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def unsupported_func(*args: Any, **kwargs: Any) -> Any:
        raise TypeError(DEFAULT_NOT_SUPPORTED_MESSAGE)

    return unsupported_func


def raise_unsupported_parameter(
    *,
    class_name: str,
    method_name: str,
    parameter_name: str,
) -> None:
    message = DEFAULT_NOT_SUPPORTED_PARAMETER_MESSAGE_TEMPLATE.format(
        class_name=class_name,
        method_name=method_name,
        parameter_name=parameter_name,
    )
    raise TypeError(message)
