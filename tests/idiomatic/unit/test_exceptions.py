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

import pytest
from httpx import HTTPStatusError, Response

from astrapy.exceptions import (
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIHttpException,
    DataAPIResponseException,
    DeleteManyException,
    InsertManyException,
)
from astrapy.results import DeleteResult, InsertManyResult


@pytest.mark.describe("test DataAPIHttpException")
def test_dataapihttpexception() -> None:
    """Test that regardless of how incorrect the input httpx error, nothing breaks."""
    se0 = HTTPStatusError(message="httpx_message", request="req", response=None)  # type: ignore[arg-type]
    se1 = HTTPStatusError(message="httpx_message", request="req", response="blah")  # type: ignore[arg-type]
    se2 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text="blah"),
    )
    se3 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text='{"blabla": 1}'),
    )
    se4 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(status_code=500, text='{"errors": []}'),
    )
    se5 = HTTPStatusError(
        message="httpx_message",
        request="req",  # type: ignore[arg-type]
        response=Response(
            status_code=500,
            text='{"errors": [{"title":"da_title","errorCode":"DA_ERRORCODE","message":"da_message"}]}',
        ),
    )

    de0 = DataAPIHttpException.from_httpx_error(se0)
    de1 = DataAPIHttpException.from_httpx_error(se1)
    de2 = DataAPIHttpException.from_httpx_error(se2)
    de3 = DataAPIHttpException.from_httpx_error(se3)
    de4 = DataAPIHttpException.from_httpx_error(se4)
    de5 = DataAPIHttpException.from_httpx_error(se5)

    repr(de0)
    repr(de1)
    repr(de2)
    repr(de3)
    repr(de4)
    repr(de5)
    str(de0)
    str(de1)
    str(de2)
    str(de3)
    str(de4)
    assert "da_message" in str(de5)


@pytest.mark.describe("test DataAPIResponseException")
def test_dataapiresponseexception() -> None:
    da_e1 = DataAPIResponseException.from_responses(
        commands=[{"cmd": "C1"}],
        raw_responses=[
            {"errors": [{"errorCode": "C", "message": "Aaa", "field": "value"}]}
        ],
    )
    the_daed = DataAPIErrorDescriptor(
        {"errorCode": "C", "message": "Aaa", "field": "value"}
    )

    assert the_daed.error_code == "C"
    assert the_daed.message == "Aaa"
    assert the_daed.attributes == {"field": "value"}

    assert da_e1.text == "Aaa"
    assert str(da_e1) == "Aaa"
    assert da_e1.error_descriptors == [the_daed]
    assert da_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={
                "errors": [{"errorCode": "C", "message": "Aaa", "field": "value"}]
            },
        )
    ]


@pytest.mark.describe("test InsertManyException")
def test_insertmanyexception() -> None:
    im_result = InsertManyResult(raw_results=[{"a": 1}], inserted_ids=["a", "b"])
    # mypy thinks im_e1 is a DataAPIException for some reason...
    im_e1: InsertManyException = InsertManyException.from_responses(  # type: ignore[assignment]
        commands=[{"cmd": "C1"}],
        raw_responses=[{"errors": [{"errorCode": "C", "message": "Aaa"}]}],
        partial_result=im_result,
    )
    the_daed = DataAPIErrorDescriptor({"errorCode": "C", "message": "Aaa"})

    assert im_e1.partial_result == im_result
    assert im_e1.text == "Aaa"
    assert im_e1.error_descriptors == [the_daed]
    assert im_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={"errors": [{"errorCode": "C", "message": "Aaa"}]},
        )
    ]


@pytest.mark.describe("test DeleteManyException")
def test_deletemanyexception() -> None:
    dm_result = DeleteResult(deleted_count=123, raw_results=[{"a": 1}])
    # mypy thinks dm_e1 is a DataAPIException for some reason...
    dm_e1: DeleteManyException = DeleteManyException.from_responses(  # type: ignore[assignment]
        commands=[{"cmd": "C1"}],
        raw_responses=[{"errors": [{"errorCode": "C", "message": "Aaa"}]}],
        partial_result=dm_result,
    )
    the_daed = DataAPIErrorDescriptor({"errorCode": "C", "message": "Aaa"})

    assert dm_e1.partial_result == dm_result
    assert dm_e1.text == "Aaa"
    assert dm_e1.error_descriptors == [the_daed]
    assert dm_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command={"cmd": "C1"},
            raw_response={"errors": [{"errorCode": "C", "message": "Aaa"}]},
        )
    ]
