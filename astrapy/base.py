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

import logging
from astrapy.defaults import DEFAULT_AUTH_HEADER

from astrapy.ops import AstraOps
from astrapy.utils import make_request

logger = logging.getLogger(__name__)

REQUESTED_WITH = "AstraPy"


class AstraClient:
    def __init__(
        self,
        base_url=None,
        debug=False,
        astra_database_id=None,
        astra_database_region=None,
        astra_application_token=None,
    ):
        # Check the base url
        # TODO: Abstract away the URL into some global config
        if base_url is None:
            base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com"
        
        # Base Parameters
        self.base_url = base_url
        self.debug = debug

        # Astra Parameters
        self.astra_database_id = astra_database_id
        self.astra_database_region = astra_database_region
        self.astra_application_token = astra_application_token

        # Start with some uninitialized functionality
        self.astra_ops = None


    def request(
            self, 
            *args, 
            **kwargs
        ):
        make_request(*args, **kwargs, 
                     base_url=self.base_url, 
                     auth_header=DEFAULT_AUTH_HEADER,
                     token=self.astra_application_token)

        
    def ops(
        self
    ):
        # Initialize AstraOps if not already done
        if not self.astra_ops:
            self.astra_ops = AstraOps(token=self.astra_application_token)

        # Return the call
        return self.astra_ops
