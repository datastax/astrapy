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

import math
from typing import Generic, Iterable, Iterator, Mapping, TypeVar

T = TypeVar("T")
U = TypeVar("U")


def _accumulate_pairs(
    destination: tuple[list[T], list[U]], source: Iterable[tuple[T, U]]
) -> tuple[list[T], list[U]]:
    _new_ks = list(destination[0])
    _new_vs = list(destination[1])
    for k, v in source:
        if k not in _new_ks:
            _new_ks.append(k)
            _new_vs.append(v)
    return (_new_ks, _new_vs)


class DataAPIMap(Generic[T, U], Mapping[T, U]):
    """
    An immutable 'map-like' class that preserves the order and can employ
    non-hashable keys (which must support __eq__). Not designed for performance.

    Despite internally preserving the order, equality between DataAPIMap instances
    (and with regular dicts) is independent of the order.
    """

    _keys: list[T]
    _values: list[U]

    def __init__(self, source: Iterable[tuple[T, U]] | dict[T, U] = []) -> None:
        if isinstance(source, dict):
            self._keys, self._values = _accumulate_pairs(
                ([], []),
                source.items(),
            )
        else:
            self._keys, self._values = _accumulate_pairs(
                ([], []),
                source,
            )

    def __getitem__(self, key: T) -> U:
        if isinstance(key, float) and math.isnan(key):
            for idx, k in enumerate(self._keys):
                if isinstance(k, float) and math.isnan(k):
                    return self._values[idx]
            raise KeyError(str(key))
        else:
            for idx, k in enumerate(self._keys):
                if k == key:
                    return self._values[idx]
            raise KeyError(str(key) + "//" + str(self._keys) + "//" + str(self._values))

    def __iter__(self) -> Iterator[T]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataAPIMap):
            if len(self) == len(other):
                if all(o_k in self for o_k in other):
                    return all(other[k] == self[k] for k in self)
            return False
        try:
            dother = dict(other)  # type: ignore[call-overload]
            return all(
                [
                    len(dother) == len(self),
                    all(o_k in self for o_k in dother),
                    all(dother[k] == self[k] for k in self),
                ]
            )
        except KeyError:
            return False
        except TypeError:
            pass
        return NotImplemented

    def __repr__(self) -> str:
        _map_repr = ", ".join(
            f"({repr(k)}, {repr(v)})" for k, v in zip(self._keys, self._values)
        )
        return f"{self.__class__.__name__}([{_map_repr}])"

    def __str__(self) -> str:
        _map_repr = ", ".join(f"({k}, {v})" for k, v in zip(self._keys, self._values))
        return f"{_map_repr}"

    def __reduce__(self) -> tuple[type, tuple[Iterable[tuple[T, U]]]]:
        return self.__class__, (list(zip(self._keys, self._values)),)
