# Copyright 2024 Google LLC
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
#
from __future__ import annotations

from typing import TYPE_CHECKING

import concurrent.futures
import atexit

from google.cloud.bigtable.data._sync._autogen import _FlowControl_SyncGen
from google.cloud.bigtable.data._sync._autogen import MutationsBatcher_SyncGen

# import required so MutationsBatcher_SyncGen can create _MutateRowsOperation
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

if TYPE_CHECKING:
    from google.cloud.bigtable.data.exceptions import FailedMutationEntryError


class MutationsBatcher:


    def __init__(self, table, **kwargs):
        from google.cloud.bigtable.data import MutationsBatcherAsync
        self._event_loop = table._event_loop
        self._batcher = self._event_loop.run_until_complete(MutationsBatcherAsync._make_one(table._async_table, **kwargs))

    def append(self, *args, **kwargs):
        return self._event_loop.run_until_complete(self._batcher.append(*args, **kwargs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._event_loop.run_until_complete(self._batcher.__aexit__(exc_type, exc_val, exc_tb))

    def __getattr__(self, name):
        return getattr(self._batcher, name)
