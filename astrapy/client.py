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

from astrapy.collections import create_client
from astrapy.endpoints.rest import AstraRest
from astrapy.endpoints.schemas import AstraSchemas
from astrapy.endpoints.ops import AstraOps
from astrapy.endpoints.graphql import AstraGraphQL
import logging

logger = logging.getLogger(__name__)


class AstraClient():
    def __init__(self, astra_collections_client=None):
        self.collections = astra_collections_client
        self._rest_client = astra_collections_client.astra_client
        self.rest = AstraRest(self._rest_client)
        self.ops = AstraOps(self._rest_client)
        self.schemas = AstraSchemas(self._rest_client)
        self.gql = AstraGraphQL(self._rest_client)


def create_astra_client(astra_database_id=None,
                        astra_database_region=None,
                        astra_application_token=None,
                        base_url=None,
                        debug=False):
    astra_collections_client = create_client(astra_database_id=astra_database_id,
                                             astra_database_region=astra_database_region,
                                             astra_application_token=astra_application_token)
    return AstraClient(astra_collections_client=astra_collections_client)
