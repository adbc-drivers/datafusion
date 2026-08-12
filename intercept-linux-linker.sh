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
    local args=("$@")

    # Rust always marks no_mangle symbols as public. The BLAKE3 crate happens
    # to have such a symbol, but only on aarch64 platforms, which gets
    # exported and trips our post-build checks. We don't want that, so find
    # and filter the version script by hand.
    # https://github.com/rust-lang/rust/issues/73958
    # https://github.com/dtolnay/cxx/pull/1520
    for i in ${!args[@]}; do
        if [[ "${args[$i]}" == -Wl,--version-script=* ]]; then
            local -r version_script="${args[$i]#*=}"
            local -r scratch=$(mktemp)
            cat "${version_script}" | grep -v 'blake3_' > "${scratch}"
            mv "${scratch}" "${version_script}"
        fi
    done

    cc "$@"
}

main "$@"
