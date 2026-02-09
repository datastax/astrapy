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

import argparse
import json
import os
from typing import Any, no_type_check


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare old and new coverage JSON data and print a report."
    )

    # Mandatory positional arguments
    parser.add_argument("old_coverage_json", help="Path to the old coverage JSON file")
    parser.add_argument("new_coverage_json", help="Path to the new coverage JSON file")

    # Optional output filename
    parser.add_argument(
        "-o",
        "--output",
        metavar="filename",
        help="Write output to a file instead of stdout",
    )

    # Optional format selector
    parser.add_argument(
        "-f",
        "--format",
        choices=["json", "table"],
        default="json",
        help="Output format (default: json)",
    )

    return parser.parse_args()


def calc_delta(prev_j0: dict[str, Any] | None, new_j: dict[str, Any]) -> dict[str, Any]:
    # Workaround to missing prev (we treat is as 'no previous coverage')
    has_delta = prev_j0 is not None
    prev_j = prev_j0 or {"totals": {"percent_covered": 0.0}, "files": {}}
    overall_delta = (
        new_j["totals"]["percent_covered"] - prev_j["totals"]["percent_covered"]
    )

    def _file_delta(pj: dict[str, Any], nj: dict[str, Any], f_name: str) -> float:
        new_coverage: float = nj["files"][f_name]["summary"]["percent_covered"]
        if f_name not in pj["files"]:
            return new_coverage
        else:
            old_coverage: float = pj["files"][f_name]["summary"]["percent_covered"]
            return new_coverage - old_coverage

    per_file_deltas: dict[str, Any] = dict(
        sorted(
            [
                (
                    new_file_k,
                    {
                        **(
                            {"delta": _file_delta(prev_j, new_j, f_name=new_file_k)}
                            if has_delta
                            else {}
                        ),
                        **new_file_v["summary"],
                    },
                )
                for new_file_k, new_file_v in new_j["files"].items()
            ],
            key=lambda file_pair: (
                file_pair[1].get("delta", 0.0),
                file_pair[1]["percent_covered"],
            ),
        )
    )
    return {
        "totals": {
            **({"delta": overall_delta} if has_delta else {}),
            **new_j["totals"],
        },
        "files": per_file_deltas,
    }


def make_json_report(r_json: dict[str, float]) -> str:
    return json.dumps(r_json, indent=2)


@no_type_check
def make_table_report(r_json: dict[str, float], print_delta: bool = True) -> str:
    TABLE_GUTTER = 3
    FMT_NUM_DECIMALS = 2
    FMT_STR = f"% {FMT_NUM_DECIMALS + 4}.{FMT_NUM_DECIMALS}f%%"
    COLUMNERS_0 = [
        ("File", lambda kp: kp[0]),
        ("Stmts", lambda kp: f"{(kp[1]['num_statements'])}"),
        ("Miss", lambda kp: f"{(kp[1]['missing_lines'])}"),
        ("Cover", lambda kp: FMT_STR % kp[1]["percent_covered"]),
    ]
    COLUMNERS_D = [
        ("Delta", lambda kp: FMT_STR % kp[1]["delta"]),
    ]
    COLUMNERS = COLUMNERS_0 + (COLUMNERS_D if print_delta else [])

    col_widths0 = [
        max(len(col_n), len(col_v(("totals", r_json["totals"]))))
        for col_n, col_v in COLUMNERS
    ]
    col_widths = [
        max(
            [len(col_v((file_k, file_v))) for file_k, file_v in r_json["files"].items()]
            + [col_head_max]
        )
        for (_, col_v), col_head_max in zip(COLUMNERS, col_widths0)
    ]

    def _mk_line(file_k, file_v):
        return (" " * TABLE_GUTTER).join(
            [
                (f"%{col_w}s") % col_v((file_k, file_v))
                for (_, col_v), col_w in zip(COLUMNERS, col_widths)
            ]
        )

    header_line = (" " * TABLE_GUTTER).join(
        [(f"%{col_w}s") % col_k for (col_k, _), col_w in zip(COLUMNERS, col_widths)]
    )
    totals_line = _mk_line("totals", r_json["totals"])
    line_width = sum(col_widths) + TABLE_GUTTER * (len(COLUMNERS) - 1)
    sep_line = "-" * line_width

    file_lines = [
        _mk_line(file_k, file_v) for file_k, file_v in r_json["files"].items()
    ]

    return "\n".join([header_line, sep_line] + file_lines + [sep_line, totals_line])


if __name__ == "__main__":
    args = parse_args()
    old_coverage: dict[str, Any] | None
    has_delta = os.path.isfile(args.old_coverage_json)
    if has_delta:
        old_coverage = json.load(open(args.old_coverage_json))
    else:
        old_coverage = None
    new_coverage = json.load(open(args.new_coverage_json))
    delta_json = calc_delta(old_coverage, new_coverage)
    report_str: str
    if args.format == "json":
        report_str = make_json_report(delta_json)
    elif args.format == "table":
        report_str = make_table_report(delta_json, print_delta=has_delta)
    else:
        raise ValueError(f"Unknown report format {args.format}.")

    if args.output is None:
        print(report_str)
    else:
        print(f"Writing report to {args.output} ...", end="")
        with open(args.output, "w") as ofile:
            ofile.write(report_str)
        print(" done.")
