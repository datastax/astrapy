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

import importlib.metadata
import os

import toml


def get_version() -> str:
    try:
        # Expect a __version__ attribute in the package's __init__.py file
        return importlib.metadata.version(__package__)

    # If the package is not installed, we can still get the version from the pyproject.toml file
    except importlib.metadata.PackageNotFoundError:
        # Get the path to the pyproject.toml file
        dir_path = os.path.dirname(os.path.realpath(__file__))
        pyproject_path = os.path.join(dir_path, "..", "pyproject.toml")

        # Read the pyproject.toml file and get the version from the appropriate section
        try:
            with open(pyproject_path, encoding="utf-8") as pyproject:
                # Load the pyproject.toml file as a dictionary
                file_contents = pyproject.read()
                pyproject_data = toml.loads(file_contents)

                # Return the version from the 'project' section
                return str(pyproject_data["tool"]["project"]["version"])

        # If the pyproject.toml file does not exist or the version is not found, return unknown
        except (FileNotFoundError, KeyError):
            return "unknown"


__version__: str = get_version()


from astrapy import api_options  # noqa: E402, F401
from astrapy.admin import (  # noqa: E402
    AstraDBAdmin,
    AstraDBDatabaseAdmin,
    DataAPIDatabaseAdmin,
)
from astrapy.client import DataAPIClient  # noqa: E402
from astrapy.collection import AsyncCollection, Collection  # noqa: E402

# A circular-import issue requires this to happen at the end of this module:
from astrapy.database import AsyncDatabase, Database  # noqa: E402
from astrapy.table import AsyncTable, Table  # noqa: E402

__all__ = [
    "AstraDBAdmin",
    "AstraDBDatabaseAdmin",
    "AsyncCollection",
    "AsyncDatabase",
    "AsyncTable",
    "Collection",
    "Database",
    "DataAPIClient",
    "DataAPIDatabaseAdmin",
    "Table",
    "__version__",
]


__pdoc__ = {
    "ids": False,
    "settings": False,
}
