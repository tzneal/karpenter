#!/usr/bin/env bash

# Copyright 2019 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail


# neeed to either vendor the packages, or check them out temporarily
SCRIPT_DIR=$(readlink -f "$(dirname $0)")
VENDOR_DIR="${SCRIPT_DIR}/../vendor/"
checkout_vendored() {
  REPO=$1
  PKG=$2
  URL=$3
  TAG=$4

  mkdir -p "${VENDOR_DIR}/${REPO}"
  cd "${VENDOR_DIR}/${REPO}"
  git clone "${URL}"
  cd "${PKG}"
  git checkout "${TAG}"
}

checkout_vendored "knative.dev" "hack" "https://github.com/knative/hack.git" "release-1.2"
checkout_vendored "k8s.io" "code-generator" "https://github.com/kubernetes/code-generator.git" "release-1.21"
checkout_vendored "knative.dev" "pkg" "https://github.com/knative/pkg.git" "release-1.2"
cd "${SCRIPT_DIR}/../"
source "${SCRIPT_DIR}/../vendor/knative.dev/hack/codegen-library.sh"
export GOFLAGS=-mod=mod


# ${CODEGEN_PKG}/generate-groups.sh "deepcopy,client,informer,lister" \
  #  knative.dev/sample-controller/pkg/client knative.dev/sample-controller/pkg/apis \
  #  "samples:v1alpha1" \
  #  --go-header-file ${REPO_ROOT_DIR}/hack/boilerplate/boilerplate.go.txt

#        controller-gen \
  #                object:headerFile="hack/boilerplate.go.txt" \
  #                crd \
  #                paths="./pkg/..." \
  #                output:crd:artifacts:config=charts/karpenter/crds

echo "=== Update Codegen for ${MODULE_NAME}"

group "Kubernetes Codegen"

"${CODEGEN_PKG}/generate-groups.sh" "all" \
 github.com/aws/karpenter/pkg/client github.com/aws/karpenter/pkg/apis \
  "provisioning:v1alpha5" \
   --go-header-file "${REPO_ROOT_DIR}/hack/boilerplate.go.txt"

group "Knative Codegen"

"${KNATIVE_CODEGEN_PKG}/hack/generate-knative.sh" "injection" \
  github.com/aws/karpenter/pkg/client github.com/aws/karpenter/pkg/apis \
  "provisioning:v1alpha5" \
   --go-header-file "${REPO_ROOT_DIR}/hack/boilerplate.go.txt"


# Update deps post code-gen
# TODO

rm -rf "${VENDOR_DIR}/"
