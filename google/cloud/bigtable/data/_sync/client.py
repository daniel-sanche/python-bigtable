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

import google.auth.credentials
import concurrent.futures

from google.cloud.bigtable.data._sync._autogen import BigtableDataClient_SyncGen
from google.cloud.bigtable.data._sync._autogen import Table_SyncGen

# import required so Table_SyncGen can create _MutateRowsOperation and _ReadRowsOperation
import google.cloud.bigtable.data._sync._read_rows  # noqa: F401
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

if TYPE_CHECKING:
    from google.cloud.bigtable.data.row import Row


class BigtableDataClient(BigtableDataClient_SyncGen):

    def __init__(self, *args, **kwargs):
        from google.cloud.bigtable.data import BigtableDataClientAsync
        super().__init__(*args, **kwargs)
        self._async_client = BigtableDataClientAsync(*args, **kwargs)

    @property
    def _executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if not hasattr(self, "_executor_instance"):
            self._executor_instance = concurrent.futures.ThreadPoolExecutor()
        return self._executor_instance

    @staticmethod
    def _client_version() -> str:
        return f"{google.cloud.bigtable.__version__}-data"

    def _start_background_channel_refresh(self) -> None:
        if (
            not self._channel_refresh_tasks
            and not self._emulator_host
            and not self._is_closed.is_set()
        ):
            for channel_idx in range(self.transport.pool_size):
                self._channel_refresh_tasks.append(
                    self._executor.submit(self._manage_channel, channel_idx)
                )

    def _execute_ping_and_warms(self, *fns) -> list[BaseException | None]:
        futures_list = [self._executor.submit(f) for f in fns]
        results_list: list[BaseException | None] = []
        for future in futures_list:
            try:
                future.result()
                results_list.append(None)
            except BaseException as e:
                results_list.append(e)
        return results_list

    def close(self) -> None:
        """
        Close the client and all associated resources

        This method should be called when the client is no longer needed.
        """
        self._is_closed.set()
        with self._executor:
            self._executor.shutdown(wait=False)
        self._channel_refresh_tasks = []
        self.transport.close()


class Table:

    def __init__(self, client, *args, **kwargs):
        from google.cloud.bigtable.data import TableAsync
        import asyncio
        self.__event_loop = asyncio.get_event_loop()
        self.client = client
        self._async_table = TableAsync(client._async_client, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._async_table, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def close(self):
        return

    def read_rows_stream(self, *args, **kwargs):
        gen = self.__event_loop.run_until_complete(
            self._async_table.read_rows_stream(*args, **kwargs)
        )
        while True:
            try:
                next_val = self.__event_loop.run_until_complete(gen.__anext__())
                yield next_val
            except StopAsyncIteration:
                break

    def read_rows(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.read_rows(*args, **kwargs)
        )

    def read_row(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.read_row(*args, **kwargs)
        )

    def read_rows_sharded(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.read_rows_sharded(*args, **kwargs)
        )

    def row_exists(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.row_exists(*args, **kwargs)
        )

    def mutate_rows(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.mutate_rows(*args, **kwargs)
        )

    def mutate_row(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.mutate_row(*args, **kwargs)
        )

    def sample_row_keys(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.sample_row_keys(*args, **kwargs)
        )

    def read_modify_write_row(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.read_modify_write_row(*args, **kwargs)
        )

    def check_and_mutate_row(self, *args, **kwargs):
        return self.__event_loop.run_until_complete(
            self._async_table.check_and_mutate_row(*args, **kwargs)
        )

    def mutations_batcher(self, *args, **kwargs):
        from google.cloud.bigtable.data._sync.mutations_batcher import MutationsBatcher
        return MutationsBatcher(self, self.__event_loop, *args, **kwargs)
