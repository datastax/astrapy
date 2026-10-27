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

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from astrapy.info import CreateTypeDefinition

from ..integration.tables import table_cql_assets


@pytest.mark.parametrize(
    ("old_fields", "new_fields"),
    [
        (None, {"a": "text"}),
        ({"a": "text"}, {"a": "text", "b": "int"}),
        ({"a": "text"}, None),
    ],
    ids=["create", "alter", "drop"],
)
def test_udt_schema_refresh_sync(
    monkeypatch: pytest.MonkeyPatch,
    old_fields: dict[str, str] | None,
    new_fields: dict[str, str] | None,
) -> None:
    session = Mock()
    elapsed = 0.0

    def advance(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    def refresh(*args: object, **kwargs: object) -> None:
        fields = new_fields if elapsed >= 1.5 else old_fields
        user_types = (
            {}
            if fields is None
            else {
                "test_udt": SimpleNamespace(
                    field_names=list(fields), field_types=list(fields.values())
                )
            }
        )
        session.cluster.metadata.keyspaces = {
            "keyspace": SimpleNamespace(user_types=user_types)
        }

    session.cluster.refresh_user_type_metadata.side_effect = refresh
    monkeypatch.setattr(
        table_cql_assets,
        "time",
        SimpleNamespace(monotonic=lambda: elapsed, sleep=advance),
    )
    expected = (
        None if new_fields is None else CreateTypeDefinition(fields=dict(new_fields))
    )

    table_cql_assets._assert_udt_definition(session, "keyspace", "test_udt", expected)

    assert elapsed >= 1.5
    assert elapsed <= table_cql_assets.SCHEMA_PROPAGATION_TIMEOUT_SECONDS


def test_udt_schema_mismatch_times_out_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    session = Mock()
    session.cluster.metadata.keyspaces = {"keyspace": SimpleNamespace(user_types={})}
    elapsed = 0.0

    def advance(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    monkeypatch.setattr(
        table_cql_assets,
        "time",
        SimpleNamespace(monotonic=lambda: elapsed, sleep=advance),
    )

    with pytest.raises(AssertionError, match="test_udt"):
        table_cql_assets._assert_udt_definition(
            session, "keyspace", "test_udt", CreateTypeDefinition(fields={"a": "text"})
        )

    assert elapsed == table_cql_assets.SCHEMA_PROPAGATION_TIMEOUT_SECONDS


def test_udt_schema_refresh_error_sync() -> None:
    session = Mock()
    session.cluster.refresh_user_type_metadata.side_effect = RuntimeError("unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        table_cql_assets._assert_udt_definition(session, "keyspace", "test_udt", None)
