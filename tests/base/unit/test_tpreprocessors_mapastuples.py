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

from astrapy.data.table import (
    map2tuple_checker_insert_many,
    map2tuple_checker_insert_one,
    map2tuple_checker_update_one,
)
from astrapy.data.utils.table_converters import preprocess_table_payload
from astrapy.data_types import DataAPIMap
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

MAP2TUPLE_NEVER_OPTIONS = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="NEVER")
)
MAP2TUPLE_ALWAYS_OPTIONS = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="ALWAYS")
)
MAP2TUPLE_DATAAPIMAPS_OPTIONS = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables="DATAAPIMAPS")
)


class TestTPreprocessorsMapsAsTuples:
    @pytest.mark.describe("test of tuple conversion as in insert_one, never convert")
    def test_map2tuple_conversion_insertone_never(self) -> None:
        payload_d = {"insertOne": {"document": {"a": {1: "x"}}}}
        expected_d = {"insertOne": {"document": {"a": {1: "x"}}}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_d == converted_d

        payload_m = {"insertOne": {"document": {"a": DataAPIMap([(1, "x")])}}}
        expected_m = {"insertOne": {"document": {"a": {1: "x"}}}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        expected_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_de == converted_de

        payload_me = {"insertOne": {"document": {"a": DataAPIMap([])}}}  # type: ignore[var-annotated]
        expected_me = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_me == converted_me

    @pytest.mark.describe("test of tuple conversion as in insert_many, never convert")
    def test_map2tuple_conversion_insertmany_never(self) -> None:
        payload_d = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        expected_d = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_d == converted_d

        payload_m = {"insertMany": {"documents": [{"a": DataAPIMap([(1, "x")])}]}}
        expected_m = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        expected_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_de == converted_de

        payload_me = {"insertMany": {"documents": [{"a": DataAPIMap([])}]}}  # type: ignore[var-annotated]
        expected_me = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_me == converted_me

    @pytest.mark.describe("test of tuple conversion as in update_one, never convert")
    def test_map2tuple_conversion_updateone_never(self) -> None:
        payload_d = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": {2: "y"}},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        expected_d = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": {2: "y"}},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_d == converted_d

        payload_m = {
            "updateOne": {
                "filter": {"f": DataAPIMap([(1, "x")])},
                "update": {
                    "$set": {"s": DataAPIMap([(2, "y")])},
                    "$unset": {"u": DataAPIMap([(3, "z")])},
                },
            }
        }
        expected_m = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": {2: "y"}},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        expected_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_de == converted_de

        payload_me = {
            "updateOne": {
                "filter": {"f": DataAPIMap([])},
                "update": {
                    "$set": {"s": DataAPIMap([])},
                    "$unset": {"u": DataAPIMap([])},
                },
            }
        }
        expected_me = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_NEVER_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_me == converted_me

    @pytest.mark.describe("test of tuple conversion as in insert_one, convert always")
    def test_map2tuple_conversion_insertone_always(self) -> None:
        payload_d = {"insertOne": {"document": {"a": {1: "x"}}}}
        expected_d = {"insertOne": {"document": {"a": [[1, "x"]]}}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_d == converted_d

        payload_m = {"insertOne": {"document": {"a": DataAPIMap([(1, "x")])}}}
        expected_m = {"insertOne": {"document": {"a": [[1, "x"]]}}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        expected_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_de == converted_de

        payload_me = {"insertOne": {"document": {"a": DataAPIMap([])}}}  # type: ignore[var-annotated]
        expected_me = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_me == converted_me

    @pytest.mark.describe("test of tuple conversion as in insert_many, convert always")
    def test_map2tuple_conversion_insertmany_always(self) -> None:
        payload_d = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        expected_d = {"insertMany": {"documents": [{"a": [[1, "x"]]}]}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_d == converted_d

        payload_m = {"insertMany": {"documents": [{"a": DataAPIMap([(1, "x")])}]}}
        expected_m = {"insertMany": {"documents": [{"a": [[1, "x"]]}]}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        expected_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_de == converted_de

        payload_me = {"insertMany": {"documents": [{"a": DataAPIMap([])}]}}  # type: ignore[var-annotated]
        expected_me = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_me == converted_me

    @pytest.mark.describe("test of tuple conversion as in update_one, convert always")
    def test_map2tuple_conversion_updateone_always(self) -> None:
        payload_d = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": {2: "y"}},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        expected_d = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": [[2, "y"]]},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_d == converted_d

        payload_m = {
            "updateOne": {
                "filter": {"f": DataAPIMap([(1, "x")])},
                "update": {
                    "$set": {"s": DataAPIMap([(2, "y")])},
                    "$unset": {"u": DataAPIMap([(3, "z")])},
                },
            }
        }
        expected_m = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": [[2, "y"]]},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        expected_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_de == converted_de

        payload_me = {
            "updateOne": {
                "filter": {"f": DataAPIMap([])},
                "update": {
                    "$set": {"s": DataAPIMap([])},
                    "$unset": {"u": DataAPIMap([])},
                },
            }
        }
        expected_me = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_ALWAYS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_me == converted_me

    @pytest.mark.describe(
        "test of tuple conversion as in insert_one, convert DataAPIMaps"
    )
    def test_map2tuple_conversion_insertone_dataapimaps(self) -> None:
        payload_d = {"insertOne": {"document": {"a": {1: "x"}}}}
        expected_d = {"insertOne": {"document": {"a": {1: "x"}}}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_d == converted_d

        payload_m = {"insertOne": {"document": {"a": DataAPIMap([(1, "x")])}}}
        expected_m = {"insertOne": {"document": {"a": [[1, "x"]]}}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        expected_de = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_de == converted_de

        payload_me = {"insertOne": {"document": {"a": DataAPIMap([])}}}  # type: ignore[var-annotated]
        expected_me = {"insertOne": {"document": {"a": {}}}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        assert expected_me == converted_me

    @pytest.mark.describe(
        "test of tuple conversion as in insert_many, convert DataAPIMaps"
    )
    def test_map2tuple_conversion_insertmany_dataapimaps(self) -> None:
        payload_d = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        expected_d = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_d == converted_d

        payload_m = {"insertMany": {"documents": [{"a": DataAPIMap([(1, "x")])}]}}
        expected_m = {"insertMany": {"documents": [{"a": [[1, "x"]]}]}}
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        expected_de = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_de == converted_de

        payload_me = {"insertMany": {"documents": [{"a": DataAPIMap([])}]}}  # type: ignore[var-annotated]
        expected_me = {"insertMany": {"documents": [{"a": {}}]}}  # type: ignore[var-annotated]
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_insert_many,
        )
        assert expected_me == converted_me

    @pytest.mark.describe(
        "test of tuple conversion as in update_one, convert DataAPIMaps"
    )
    def test_map2tuple_conversion_updateone_dataapimaps(self) -> None:
        payload_d = {
            "updateOne": {
                "filter": {"f": {1: "g"}},
                "update": {
                    "$set": {"s": {10: "t"}},
                    "$unset": {"u": {10: "v"}},
                },
            }
        }
        expected_d = {
            "updateOne": {
                "filter": {"f": {1: "g"}},
                "update": {
                    "$set": {"s": {10: "t"}},
                    "$unset": {"u": {10: "v"}},
                },
            }
        }
        converted_d = preprocess_table_payload(
            payload_d,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_d == converted_d

        payload_m = {
            "updateOne": {
                "filter": {"f": DataAPIMap([(1, "x")])},
                "update": {
                    "$set": {"s": DataAPIMap([(2, "y")])},
                    "$unset": {"u": DataAPIMap([(3, "z")])},
                },
            }
        }
        expected_m = {
            "updateOne": {
                "filter": {"f": {1: "x"}},
                "update": {
                    "$set": {"s": [[2, "y"]]},
                    "$unset": {"u": {3: "z"}},
                },
            }
        }
        converted_m = preprocess_table_payload(
            payload_m,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_m == converted_m

        # empty 'maps' must always encode as {}
        payload_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        expected_de = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_de = preprocess_table_payload(
            payload_de,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_de == converted_de

        payload_me = {
            "updateOne": {
                "filter": {"f": DataAPIMap([])},
                "update": {
                    "$set": {"s": DataAPIMap([])},
                    "$unset": {"u": DataAPIMap([])},
                },
            }
        }
        expected_me = {
            "updateOne": {
                "filter": {"f": {}},
                "update": {
                    "$set": {"s": {}},
                    "$unset": {"u": {}},
                },
            }
        }
        converted_me = preprocess_table_payload(
            payload_me,
            MAP2TUPLE_DATAAPIMAPS_OPTIONS,
            map2tuple_checker=map2tuple_checker_update_one,
        )
        assert expected_me == converted_me
