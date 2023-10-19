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

from astrapy.ops import AstraDbOps
from astrapy.utils import make_request

logger = logging.getLogger(__name__)

REQUESTED_WITH = "AstraPy"


class AstraDbClient:
    def __init__(
        self,
        db_id,
        token,
        db_region=None,
        debug=False,
    ):
        # Base Parameters
        self.debug = debug

        # Astra Parameters
        self.db_id = db_id
        self.token = token

        # Handle the region parameter
        if not db_region:
            db_region = AstraDbOps(token=token).get_database(self.db_id)["info"]["region"]
        self.db_region = db_region

        # Set the Base URL for the API calls
        self.base_url = f"https://{self.db_id}-{self.db_region}.apps.astra.datastax.com"

        # Start with some uninitialized functionality
        self.astra_ops = None
        self.astra_vector_database = None

    def request(self, *args, **kwargs):
        result = make_request(
            *args,
            **kwargs,
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.token,
        )

        return result

    def ops(self):
        # Initialize AstraOps if not already done
        if not self.astra_ops:
            self.astra_ops = AstraDbOps(token=self.token)

        # Return the call
        return self.astra_ops

    def vector_database(self):
        # Initialize AstraOps if not already done
        if not self.astra_vector_database:
            from astrapy.collections import AstraDb

            self.astra_vector_database = AstraDb(self)

        # Return the call
        return self.astra_vector_database
