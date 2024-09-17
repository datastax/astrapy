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

import inspect
import warnings

from deprecation import DeprecatedWarning

# To count the stack frames, all aliases of 'core' must be counted"
DEPRECATED_MODULE_PATHS = {
    "astrapy.api",
    "astrapy.core",
    "astrapy.db",
    "astrapy.ops",
}


def issue_deprecation_warning() -> None:
    """
    Issue a DeprecatedWarning (subclass of DeprecationWarning).

    In order to correctly refer to user code, this function needs to
    dynamically adjust the `stacklevel` parameter.
    """

    # Get the current call stack (frames)
    stacklevel = 0
    current_stack = inspect.stack()

    # Look for the first out-of-module frame in the stack (skipping importlib stuff)
    for frame_info in current_stack:
        module_name = frame_info.frame.f_globals.get("__name__", "")
        if not module_name.startswith("importlib"):
            stacklevel = stacklevel + 1
            if all(
                not module_name.startswith(mod_path)
                for mod_path in DEPRECATED_MODULE_PATHS
            ):
                the_warning = DeprecatedWarning(
                    (
                        "All of 'astrapy.core.*', 'astrapy.api.*', "
                        "'astrapy.db.*' and 'astrapy.ops.*'"
                    ),
                    deprecated_in="1.5.0",
                    removed_in="2.0.0",
                    details=(
                        "Please refer to https://docs.datastax.com/en/astra-db-"
                        "serverless/api-reference/dataapiclient.html to start "
                        "the (recommended) path forward to the current API, and"
                        " to https://github.com/datastax/astrapy?tab=readme-ov-"
                        "file#appendix-b-compatibility-with-pre-100-library "
                        "for details on the deprecated modules."
                    ),
                )
                warnings.warn(
                    the_warning,
                    stacklevel=stacklevel,
                )
                break


issue_deprecation_warning()
