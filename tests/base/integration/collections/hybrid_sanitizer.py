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

import os

from astrapy.data_types import DataAPIVector


def _sanitize_dev_hybrid_clause(
    hybrid_clause: dict[str, str | dict[str, str | list[float] | DataAPIVector]],
) -> dict[str, str | dict[str, str | list[float] | DataAPIVector]]:
    # If not requested to do anything, do nothing:
    if "ASTRAPY_TEST_FINDANDRERANK_SUPPRESS_LEXICAL" not in os.environ:
        return hybrid_clause
    else:
        hybrid_content = hybrid_clause["$hybrid"]
        # sanitize for DEV, i.e. set $lexical to None at all costs
        if isinstance(hybrid_content, str):
            # A string can only be for vectorize. Recast as lex/vectorize
            return {
                "$hybrid": {
                    "$vectorize": hybrid_content,
                    "$lexical": None,  # type: ignore[dict-item]
                }
            }
        else:
            # A dict. Replace lexical and this is it
            assert "$lexical" in hybrid_content
            assert len(hybrid_content) == 2
            assert hybrid_content.keys() & {"$vectorize", "$vector"}
            return {
                "$hybrid": {
                    **hybrid_content,
                    **{"$lexical": None},  # type: ignore[dict-item]
                }
            }
