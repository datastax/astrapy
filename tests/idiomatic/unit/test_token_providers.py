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
Unit tests for the token providers
"""

from __future__ import annotations

import pytest

from astrapy.authentication import (
    StaticTokenProvider,
    UsernamePasswordTokenProvider,
    coerce_token_provider,
)


@pytest.mark.describe("test of static token provider")
def test_static_token_provider() -> None:
    literal_t = "AstraCS:1"
    static_tp = StaticTokenProvider(literal_t)

    assert static_tp.get_token() == literal_t


@pytest.mark.describe("test of username-password token provider")
def test_username_password_token_provider() -> None:
    up_tp = UsernamePasswordTokenProvider("cassandraA", "cassandraB")

    assert up_tp.get_token() == "Cassandra:Y2Fzc2FuZHJhQQ==:Y2Fzc2FuZHJhQg=="


@pytest.mark.describe("test of null token provider")
def test_null_token_provider() -> None:
    null_tp = StaticTokenProvider(None)

    assert null_tp.get_token() is None


@pytest.mark.describe("test of token providers coercion")
def test_coerce_token_provider() -> None:
    literal_t = "AstraCS:1"
    static_tp = StaticTokenProvider(literal_t)
    null_tp = StaticTokenProvider(None)
    up_tp = UsernamePasswordTokenProvider("cassandraA", "cassandraB")

    assert coerce_token_provider(literal_t).get_token() == literal_t
    assert coerce_token_provider(static_tp).get_token() == static_tp.get_token()
    assert coerce_token_provider(up_tp).get_token() == up_tp.get_token()
    assert coerce_token_provider(null_tp).get_token() is None


@pytest.mark.describe("test of token providers equality")
def test_token_provider_equality() -> None:
    literal_t = "AstraCS:1"
    static_tp_1 = StaticTokenProvider(literal_t)
    null_tp_1 = StaticTokenProvider(None)
    up_tp_1 = UsernamePasswordTokenProvider("cassandraA", "cassandraB")
    static_tp_2 = StaticTokenProvider(literal_t)
    null_tp_2 = StaticTokenProvider(None)
    up_tp_2 = UsernamePasswordTokenProvider("cassandraA", "cassandraB")

    assert static_tp_1 == static_tp_2
    assert null_tp_1 == null_tp_2
    assert up_tp_1 == up_tp_2

    assert static_tp_1 != null_tp_1
    assert static_tp_1 != up_tp_1
    assert null_tp_1 != static_tp_1
    assert null_tp_1 != up_tp_1
    assert up_tp_1 != static_tp_1
    assert up_tp_1 != null_tp_1

    # effective equality
    assert up_tp_1 == StaticTokenProvider("Cassandra:Y2Fzc2FuZHJhQQ==:Y2Fzc2FuZHJhQg==")


@pytest.mark.describe("test of token provider inheritance yield")
def test_token_provider_inheritance_yield() -> None:
    static_tp = StaticTokenProvider("AstraCS:xyz")
    null_tp = StaticTokenProvider(None)
    up_tp = UsernamePasswordTokenProvider("cassandraA", "cassandraB")

    assert static_tp | static_tp == static_tp
    assert static_tp | null_tp == static_tp
    assert static_tp | up_tp == static_tp

    assert null_tp | static_tp == static_tp
    assert null_tp | null_tp == null_tp
    assert null_tp | up_tp == up_tp

    assert up_tp | static_tp == up_tp
    assert up_tp | null_tp == up_tp
    assert up_tp | up_tp == up_tp

    assert static_tp or static_tp == static_tp
    assert static_tp or null_tp == static_tp
    assert static_tp or up_tp == static_tp

    assert null_tp or static_tp == static_tp
    assert null_tp or null_tp == null_tp
    assert null_tp or up_tp == up_tp

    assert up_tp or static_tp == up_tp
    assert up_tp or null_tp == up_tp
    assert up_tp or up_tp == up_tp
