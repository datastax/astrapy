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

from typing import AbstractSet, Any, Generic, Iterable, Iterator, TypeVar

T = TypeVar("T")


def _accumulate(destination: list[T], source: Iterable[T]) -> list[T]:
    _new_destination = list(destination)
    for item in source:
        if item not in _new_destination:
            _new_destination.append(item)
    return _new_destination


class DataAPISet(Generic[T], AbstractSet[T]):
    """
    An immutable 'set-like' class that preserves the order and can store
    non-hashable entries (entries must support __eq__). Not designed for performance.

    Despite internally preserving the order, equality between DataAPISet instances
    (and with regular sets) is independent of the order.
    """

    _items: list[T]

    def __init__(self, items: Iterable[T] = []) -> None:
        self._items = _accumulate([], items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, i: int) -> T:
        return self._items[i]

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __reversed__(self) -> Iterable[T]:
        return reversed(self._items)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._items})"

    def __reduce__(self) -> tuple[type, tuple[Iterable[T]]]:
        return self.__class__, (self._items,)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, (set, DataAPISet)):
            return len(other) == len(self._items) and all(
                item in self for item in other
            )
        else:
            return NotImplemented

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self._items != other._items
        else:
            try:
                return len(other) != len(self._items) or any(
                    item not in self for item in other
                )
            except TypeError:
                return NotImplemented

    def __le__(self, other: Any) -> bool:
        return self.issubset(other)

    def __lt__(self, other: Any) -> bool:
        return len(other) > len(self._items) and self.issubset(other)

    def __ge__(self, other: Any) -> bool:
        return self.issuperset(other)

    def __gt__(self, other: Any) -> bool:
        return len(self._items) > len(other) and self.issuperset(other)

    def __and__(self, other: Any) -> DataAPISet[T]:
        return self._intersect(other)

    __rand__ = __and__

    def __or__(self, other: Any) -> DataAPISet[T]:
        return self.union(other)

    __ror__ = __or__

    def __sub__(self, other: Any) -> DataAPISet[T]:
        return self._diff(other)

    def __rsub__(self, other: Any) -> DataAPISet[T]:
        return DataAPISet(other) - self

    def __xor__(self, other: Any) -> DataAPISet[T]:
        return self.symmetric_difference(other)

    __rxor__ = __xor__

    def __contains__(self, item: Any) -> bool:
        return item in self._items

    def isdisjoint(self, other: Any) -> bool:
        return len(self._intersect(other)) == 0

    def issubset(self, other: Any) -> bool:
        return len(self._intersect(other)) == len(self._items)

    def issuperset(self, other: Any) -> bool:
        return len(self._intersect(other)) == len(other)

    def union(self, *others: Any) -> DataAPISet[T]:
        return DataAPISet(
            _accumulate(list(iter(self)), (item for other in others for item in other)),
        )

    def intersection(self, *others: Any) -> DataAPISet[T]:
        isect = DataAPISet(iter(self))
        for other in others:
            isect = isect._intersect(other)
            if not isect:
                break
        return isect

    def difference(self, *others: Any) -> DataAPISet[T]:
        diff = DataAPISet(iter(self))
        for other in others:
            diff = diff._diff(other)
            if not diff:
                break
        return diff

    def symmetric_difference(self, other: Any) -> DataAPISet[T]:
        diff_self_other = self._diff(other)
        diff_other_self = other.difference(self)
        return diff_self_other.union(diff_other_self)

    def _diff(self, other: Any) -> DataAPISet[T]:
        return DataAPISet(
            _accumulate(
                [],
                (item for item in self._items if item not in other),
            )
        )

    def _intersect(self, other: Any) -> DataAPISet[T]:
        return DataAPISet(
            _accumulate(
                [],
                (item for item in self._items if item in other),
            )
        )
