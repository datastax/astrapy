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


class DataAPIException(ValueError):
    """
    Any exception occurred while issuing requests to the Data API
    and specific to it, such as:
      - a collection is found not to exist when gettings its metadata,
      - the API return a response with an error,
    but not, for instance,
      - a network error while sending an HTTP request to the API.
    """

    pass
