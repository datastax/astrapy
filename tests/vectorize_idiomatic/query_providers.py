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

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from typing import Dict

from live_provider_info import live_provider_info

from astrapy.info import EmbeddingProvider, EmbeddingProviderParameter


def desc_param(param_data: EmbeddingProviderParameter) -> str:
    if param_data.parameter_type.lower() == "string":
        return "str"
    elif param_data.parameter_type.lower() == "number":
        validation = param_data.validation
        if "numericRange" in validation:
            validation_nr = validation["numericRange"]
            assert isinstance(validation_nr, list) and len(validation_nr) == 2
            range_desc = f"[{validation_nr[0]} : {validation_nr[1]}]"
            if param_data.default_value is not None:
                range_desc2 = f"{range_desc} (default={param_data.default_value})"
            else:
                range_desc2 = range_desc
            return f"number, {range_desc2}"
        elif "options" in validation:
            validation_op = validation["options"]
            assert isinstance(validation_op, list) and len(validation_op) > 1
            return f"number, {' / '.join(str(v) for v in validation_op)}"
        else:
            raise ValueError(
                f"Unknown number validation spec: '{json.dumps(validation)}'"
            )
    elif param_data.parameter_type.lower() == "boolean":
        return "bool"
    else:
        raise NotImplementedError


if __name__ == "__main__":
    providers: Dict[str, EmbeddingProvider] = live_provider_info()
    providers_json = {ep_name: ep.as_dict() for ep_name, ep in providers.items()}
    json.dump(providers_json, open("_providers.json", "w"), indent=2, sort_keys=True)

    for provider, provider_data in sorted(providers.items()):
        print(f"{provider} ({len(provider_data.models)} models)")
        print("    auth:")
        for auth_type, auth_data in sorted(
            provider_data.supported_authentication.items()
        ):
            if auth_data.enabled:
                tokens = ", ".join(f"'{tok.accepted}'" for tok in auth_data.tokens)
                print(f"      {auth_type} ({tokens})")
        if provider_data.parameters:
            print("    parameters")
            for param_data in provider_data.parameters:
                param_name = param_data.name
                if param_data.required:
                    param_display_name = param_name
                else:
                    param_display_name = f"({param_name})"
                param_desc = desc_param(param_data)
                print(f"      - {param_display_name}: {param_desc}")
        print("    models:")
        for model_data in sorted(provider_data.models, key=lambda pro: pro.name):
            model_name = model_data.name
            if model_data.vector_dimension is not None:
                assert model_data.vector_dimension > 0
                model_dim_desc = f" (D = {model_data.vector_dimension})"
            else:
                model_dim_desc = ""
            if True:
                print(f"      {model_name}{model_dim_desc}")
                if model_data.parameters:
                    for param_data in model_data.parameters:
                        param_name = param_data.name
                        if param_data.required:
                            param_display_name = param_name
                        else:
                            param_display_name = f"({param_name})"
                        param_desc = desc_param(param_data)
                        print(f"        - {param_display_name}: {param_desc}")
