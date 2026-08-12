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
    # Rust always marks no_mangle symbols as public. On macOS, rustc passes
    # those symbols to the linker in an exported symbols list. Replace that
    # list with a wildcard for the public ADBC entry points so symbols from
    # native dependencies (such as BLAKE3 on aarch64) remain hidden.
    # https://github.com/rust-lang/rust/issues/73958
    local previous_arg=""
    for arg in "$@"; do
        local exported_symbols_list=""
        if [[ "${previous_arg}" == -Wl,-exported_symbols_list && "${arg}" == -Wl,* ]]; then
            exported_symbols_list="${arg#-Wl,}"
        elif [[ "${arg}" == -Wl,-exported_symbols_list,* ]]; then
            exported_symbols_list="${arg#-Wl,-exported_symbols_list,}"
        fi

        if [[ -n "${exported_symbols_list}" ]]; then
            local scratch
            scratch=$(mktemp)
            printf '%s\n' '_Adbc*' > "${scratch}"
            mv "${scratch}" "${exported_symbols_list}"
        fi

        previous_arg="${arg}"
    done

    cc "$@"
}

main "$@"
