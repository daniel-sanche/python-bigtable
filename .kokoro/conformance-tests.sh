#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eox pipefail


cd python-bigtable/test_proxy

# Disable buffering, so that the logs stream through.
export PYTHONUNBUFFERED=1

# Debug: show build environment
env | grep KOKORO

# Install nox
python3.9 -m pip install --upgrade --quiet nox

# Setup service account credentials.
export GOOGLE_APPLICATION_CREDENTIALS=${KOKORO_GFILE_DIR}/service-account.json
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

# Setup project id.
export PROJECT_ID=$(cat "${KOKORO_GFILE_DIR}/project-id.json")
gcloud config set project $PROJECT_ID

export PROXY_PORT=50055

# download conformance tests
git clone https://github.com/googleapis/cloud-bigtable-clients-test.git

# start up test proxy
python test_proxy.py --port $PROXY_PORT &

# run tests
pushd cloud-bigtable-clients-test/tests
set +e
go test -v -proxy_addr=:$PROXY_PORT
TEST_STATUS_CODE=$?

# exit with proper status code
exit $TEST_STATUS_CODE
