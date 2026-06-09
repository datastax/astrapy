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

"""
Entrypoint for vectorize switch management.

Env. vars are read and exposed, and the choice of the preferred
vectorize provider is set here for all tests in /base.
"""

from __future__ import annotations

import os

HEADER_EMBEDDING_API_KEY_VOYAGEAI = os.environ.get("HEADER_EMBEDDING_API_KEY_VOYAGEAI")

# VECTORIZE PROVIDER SWITCHER FOR base TESTS - HARDCODED IN THIS MODULE:
# This controls the actual (auth-requiring) provider being used for the 'base' vectorize tests.
# Provider, model name, dimension and API Key (from env. variable) must match.
EMBEDDING_PROVIDER_NAME = "voyageAI"
EMBEDDING_PROVIDER_MODEL_NAME = "voyage-2"
EMBEDDING_PROVIDER_API_KEY = HEADER_EMBEDDING_API_KEY_VOYAGEAI
EMBEDDING_PROVIDER_DIMENSION = 1024
# Moreover, for Astra DB KSM testing (if enabled), the database must be scoped
# a matching secret with this name:
EMBEDDING_PROVIDER_SHARED_SECRET_KEY_NAME = "SHARED_SECRET_EMBEDDING_API_KEY_VOYAGEAI"

__all__ = [
    "EMBEDDING_PROVIDER_NAME",
    "EMBEDDING_PROVIDER_MODEL_NAME",
    "EMBEDDING_PROVIDER_API_KEY",
    "EMBEDDING_PROVIDER_DIMENSION",
    "EMBEDDING_PROVIDER_SHARED_SECRET_KEY_NAME",
]
