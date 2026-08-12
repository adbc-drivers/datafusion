#!/bin/bash
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

main() {
    local args=()
    local found_exported_symbol=false

    # Rust always marks no_mangle symbols as public. On macOS, rustc passes
    # each such symbol to the linker with -exported_symbol. Replace that list
    # with a wildcard for the public ADBC entry points so symbols from native
    # dependencies (such as BLAKE3 on aarch64) remain hidden.
    # https://github.com/rust-lang/rust/issues/73958
    for arg in "$@"; do
        if [[ "${arg}" == -Wl,-exported_symbol,* ]]; then
            found_exported_symbol=true
        else
            args+=("${arg}")
        fi
    done

    if [[ "${found_exported_symbol}" == true ]]; then
        args+=("-Wl,-exported_symbol,_Adbc*")
    fi

    cc "${args[@]}"
}

main "$@"
