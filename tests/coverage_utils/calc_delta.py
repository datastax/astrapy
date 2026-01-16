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
from operator import itemgetter
from typing import Any

TABLE_GUTTER = 3
FMT_NUM_DECIMALS = 2


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


def calc_delta(prev_j: dict[str, Any], new_j: dict[str, Any]) -> dict[str, Any]:
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

    per_file_deltas = dict(
        sorted(
            [
                (new_file, _file_delta(prev_j, new_j, f_name=new_file))
                for new_file in new_j["files"].keys()
            ],
            key=itemgetter(1),
        )
    )
    return {
        "OVERALL": overall_delta,
        **per_file_deltas,
    }


def make_json_report(r_json: dict[str, float]) -> str:
    return json.dumps(r_json, indent=2)


def make_table_report(r_json: dict[str, float]) -> str:
    num_width = FMT_NUM_DECIMALS + 4
    fmt_str = f"% {num_width}.{FMT_NUM_DECIMALS}f"
    #
    title_width = max(len(k) for k in r_json.keys()) + TABLE_GUTTER
    row_width = title_width + TABLE_GUTTER + num_width
    title_heading = (f"%-{title_width}s") % "File"
    heading = f"{title_heading}{' ' * TABLE_GUTTER}{' ' * num_width}"
    spacer = "-" * row_width
    rows = [
        f"%-{title_width}s{' ' * TABLE_GUTTER}{fmt_str}" % (delta_title, delta_percent)
        for delta_title, delta_percent in r_json.items()
    ]
    return "\n".join([heading, spacer] + rows)


if __name__ == "__main__":
    args = parse_args()
    old_coverage = json.load(open(args.old_coverage_json))
    new_coverage = json.load(open(args.new_coverage_json))
    delta_json = calc_delta(old_coverage, new_coverage)
    report_str: str
    if args.format == "json":
        report_str = make_json_report(delta_json)
    elif args.format == "table":
        report_str = make_table_report(delta_json)
    else:
        raise ValueError(f"Unknown report format {args.format}.")

    if args.output is None:
        print(report_str)
    else:
        print(f"Writing report to {args.output} ...", end="")
        with open(args.output, "w") as ofile:
            ofile.write(report_str)
        print(" done.")
