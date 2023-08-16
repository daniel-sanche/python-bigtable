# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import os
import pathlib

import nox


DEFAULT_PYTHON_VERSION = "3.10"

PROXY_SERVER_PORT = os.environ.get("PROXY_SERVER_PORT", "50055")
PROXY_CLIENT_VERSION = os.environ.get("PROXY_CLIENT_VERSION", None)

CURRENT_DIRECTORY = pathlib.Path(__file__).parent.absolute()
REPO_ROOT_DIRECTORY = CURRENT_DIRECTORY.parent

nox.options.sessions = ["run_proxy", "conformance_tests"]

TEST_REPO_URL = "https://github.com/googleapis/cloud-bigtable-clients-test.git"
CLONE_REPO_DIR = "cloud-bigtable-clients-test"

# Error if a python version is missing
nox.options.error_on_missing_interpreters = True


@nox.session(python=DEFAULT_PYTHON_VERSION)
def run_proxy(session):
    default(session)


@nox.session(python=DEFAULT_PYTHON_VERSION)
def conformance_tests(session):
    # download the conformance test suite
    clone_dir = os.path.join(CURRENT_DIRECTORY, CLONE_REPO_DIR)
    if not os.path.exists(clone_dir):
        print("downloading copy of test repo")
        session.run("git", "clone", TEST_REPO_URL, CLONE_REPO_DIR)
    # time.sleep(10)
    with session.chdir(f"{clone_dir}/tests"):
        session.run("go", "test", "-v", f"-proxy_addr=:{PROXY_SERVER_PORT}")


def default(session):
    """Run the performance test suite."""
    if PROXY_CLIENT_VERSION is not None:
        # install released version of the library
        session.install(f"python-bigtable=={PROXY_CLIENT_VERSION}")
    else:
        # install the library from the source
        session.install("-e", str(REPO_ROOT_DIRECTORY))

    session.run("python", "test_proxy.py", "--port", PROXY_SERVER_PORT, *session.posargs,)
