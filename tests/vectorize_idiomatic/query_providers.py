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

import json
import os
import sys

from typing import Any, Dict

from astrapy.api_commander import APICommander


def desc_param(param_data: Dict[str, Any]) -> str:
    if param_data["type"].lower() == "string":
        return "str"
    elif param_data["type"].lower() == "number":
        validation = param_data.get("validation", {}).get("numericRange")
        assert isinstance(validation, list) and len(validation) == 2
        range_desc = f"[{validation[0]} : {validation[1]}]"
        if "defaultValue" in param_data:
            range_desc2 = f"{range_desc} (default={param_data['defaultValue']})"
        else:
            range_desc2 = range_desc
        return f"number, {range_desc2}"
    elif param_data["type"].lower() == "boolean":
        return "bool"
    else:
        raise NotImplementedError


if __name__ == "__main__":
    if "l" in sys.argv[1:]:
        response = json.load(open("_providers.json"))
    else:
        ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]
        api_endpoint = ASTRA_DB_API_ENDPOINT
        path = "api/json/v1"
        headers = {"Token": ASTRA_DB_APPLICATION_TOKEN}
        cmd = APICommander(
            api_endpoint=api_endpoint,
            path=path,
            headers=headers,
        )
        response = cmd.request(payload={"findEmbeddingProviders": {}})
        json.dump(response, open("_providers.json", "w"), indent=2, sort_keys=True)

    provider_map = response["status"]["embeddingProviders"]
    for provider, provider_data in sorted(provider_map.items()):
        print(provider)
        print("    auth:")
        for auth_type, auth_data in sorted(
            provider_data["supportedAuthentication"].items()
        ):
            if auth_data["enabled"]:
                tokens = ", ".join(
                    f"'{tok['accepted']}'" for tok in auth_data["tokens"]
                )
                print(f"      {auth_type} ({tokens})")
        if provider_data.get("parameters"):
            print("    parameters")
            for param_data in provider_data["parameters"]:
                param_name = param_data["name"]
                if param_data["required"]:
                    param_display_name = param_name
                else:
                    param_display_name = f"({param_name})"
                param_desc = desc_param(param_data)
                print(f"      - {param_display_name}: {param_desc}")
        print("    models:")
        for model_data in sorted(provider_data["models"], key=lambda pro: pro["name"]):
            model_name = model_data["name"]
            if model_data["vectorDimension"] > 0:
                model_dim_desc = f" (D = {model_data['vectorDimension']})"
            else:
                model_dim_desc = ""
            if True:
                print(f"      {model_name}{model_dim_desc}")
                if model_data.get("parameters"):
                    for param_data in model_data["parameters"]:
                        param_name = param_data["name"]
                        if param_data["required"]:
                            param_display_name = param_name
                        else:
                            param_display_name = f"({param_name})"
                        param_desc = desc_param(param_data)
                        print(f"        - {param_display_name}: {param_desc}")
