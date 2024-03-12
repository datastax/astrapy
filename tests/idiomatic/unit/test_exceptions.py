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

import pytest

from astrapy.exceptions import (
    InsertManyException,
    DataAPIException,
    DataAPIErrorDescriptor,
    DataAPIDetailedErrorDescriptor,
)


@pytest.mark.describe("test DataAPIException")
def test_dataapiexception() -> None:
    da_e1 = DataAPIException.from_responses(
        commands=["C1"],
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
            command="C1",
            raw_response={
                "errors": [{"errorCode": "C", "message": "Aaa", "field": "value"}]
            },
        )
    ]


@pytest.mark.describe("test InsertManyException")
def test_insertmanyexception() -> None:
    im_e1 = InsertManyException.from_responses(
        commands=["C1"],
        raw_responses=[{"errors": [{"errorCode": "C", "message": "Aaa"}]}],
        partial_result="PR",
    )
    the_daed = DataAPIErrorDescriptor({"errorCode": "C", "message": "Aaa"})

    assert im_e1.partial_result == "PR"
    assert im_e1.text == "Aaa"
    assert im_e1.error_descriptors == [the_daed]
    assert im_e1.detailed_error_descriptors == [
        DataAPIDetailedErrorDescriptor(
            error_descriptors=[the_daed],
            command="C1",
            raw_response={"errors": [{"errorCode": "C", "message": "Aaa"}]},
        )
    ]
