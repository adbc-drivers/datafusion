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

set -euo pipefail

: ${PROTOC_TAG:=v33.5}

main() {
    # "test" or "release"
    local -r config="${1}"
    local -r platform="${2}"
    local -r arch="${3}"

    local -r dest="$(pwd)/protoc"

    case "${platform}" in
        linux)
            case "${arch}" in
                amd64)
                    gh release download \
                       --repo protocolbuffers/protobuf \
                       --pattern protoc-33.5-linux-x86_64.zip \
                       --output protoc.zip \
                       "${PROTOC_TAG}"
                    ;;
                arm64)
                    gh release download \
                       --repo protocolbuffers/protobuf \
                       --pattern protoc-33.5-linux-aarch_64.zip \
                       --output protoc.zip \
                       "${PROTOC_TAG}"
                    ;;
            esac
            ;;
        macos)
            gh release download \
               --repo protocolbuffers/protobuf \
               --pattern protoc-33.5-osx-aarch_64.zip \
               --output protoc.zip \
               "${PROTOC_TAG}"
            ;;

        windows)
            gh release download \
               --repo protocolbuffers/protobuf \
               --pattern protoc-33.5-win64.zip \
               --output protoc.zip \
               "${PROTOC_TAG}"
            ;;
    esac

    unzip protoc.zip -d "${dest}"

    if [[ "${platform}" == "linux" ]] && [[ "${config}" == "release" ]]; then
        # Path needs to be path inside the container
        echo "export PROTOC=/source/protoc/bin/protoc" | tee .env.build
    else
        echo "export PROTOC=${dest}/bin/protoc" | tee .env.build
    fi
}

main "$@"
