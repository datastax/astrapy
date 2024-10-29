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

import hashlib
import json
from typing import (
    Any,
    Callable,
    Iterable,
)

from astrapy.utils.api_options import FullWireFormatOptions
from astrapy.utils.transform_payload import normalize_payload_value

IndexPairType = tuple[str, int | None]


def _maybe_valid_list_index(key_block: str) -> int | None:
    # '0', '1' is good. '00', '01', '-30' are not.
    try:
        kb_index = int(key_block)
        if kb_index >= 0 and key_block == str(kb_index):
            return kb_index
        else:
            return None
    except ValueError:
        return None


def _create_document_key_extractor(
    key: str,
) -> Callable[[dict[str, Any]], Iterable[Any]]:
    key_blocks0: list[IndexPairType] = [
        (kb_str, _maybe_valid_list_index(kb_str)) for kb_str in key.split(".")
    ]
    if key_blocks0 == []:
        raise ValueError("Field path specification cannot be empty")
    if any(kb[0] == "" for kb in key_blocks0):
        raise ValueError("Field path components cannot be empty")

    def _extract_with_key_blocks(
        key_blocks: list[IndexPairType], value: Any
    ) -> Iterable[Any]:
        if key_blocks == []:
            if isinstance(value, list):
                for item in value:
                    yield item
            else:
                yield value
            return
        else:
            # go deeper as requested
            rest_key_blocks = key_blocks[1:]
            key_block = key_blocks[0]
            k_str, k_int = key_block
            if isinstance(value, dict):
                if k_str in value:
                    new_value = value[k_str]
                    for item in _extract_with_key_blocks(rest_key_blocks, new_value):
                        yield item
                return
            elif isinstance(value, list):
                if k_int is not None:
                    if len(value) > k_int:
                        new_value = value[k_int]
                        for item in _extract_with_key_blocks(
                            rest_key_blocks, new_value
                        ):
                            yield item
                    else:
                        # list has no such element. Nothing to extract.
                        return
                else:
                    for item in value:
                        for item in _extract_with_key_blocks(key_blocks, item):
                            yield item
                return
            else:
                # keyblocks are deeper than the document. Nothing to extract.
                return

    def _item_extractor(document: dict[str, Any]) -> Iterable[Any]:
        return _extract_with_key_blocks(key_blocks=key_blocks0, value=document)

    return _item_extractor


def _reduce_distinct_key_to_safe(distinct_key: str) -> str:
    """
    In light of the twofold interpretation of "0" as index and dict key
    in selection (for distinct), and the auto-unroll of lists, it is not
    safe to go beyond the first level. See this example:
        document = {'x': [{'y': 'Y', '0': 'ZERO'}]}
        key = "x.0"
    With full key as projection, we would lose the `"y": "Y"` part (mistakenly).
    """
    blocks = distinct_key.split(".")
    valid_portion = []
    for block in blocks:
        if _maybe_valid_list_index(block) is None:
            valid_portion.append(block)
        else:
            break
    return ".".join(valid_portion)


def _hash_document(document: dict[str, Any], options: FullWireFormatOptions) -> str:
    _normalized_item = normalize_payload_value(path=[], value=document, options=options)
    _normalized_json = json.dumps(
        _normalized_item, sort_keys=True, separators=(",", ":")
    )
    _item_hash = hashlib.md5(_normalized_json.encode()).hexdigest()
    return _item_hash
