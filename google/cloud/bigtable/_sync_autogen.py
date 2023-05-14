# Copyright 2023 Google LLC
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

# This file is automatically generated by converter.py. Do not edit.


from __future__ import annotations
from functools import partial
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import cast
import asyncio
import grpc
import time
import warnings

from google.api_core import client_options as client_options_lib
from google.api_core import exceptions as core_exceptions
from google.api_core import retry as retries
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigtable import RowKeySamples
from google.cloud.bigtable._read_rows import _StateMachine
from google.cloud.bigtable.exceptions import InvalidChunk
from google.cloud.bigtable.exceptions import RetryExceptionGroup
from google.cloud.bigtable.mutations import BulkMutationsEntry
from google.cloud.bigtable.mutations import Mutation
from google.cloud.bigtable.mutations_batcher import MutationsBatcher
from google.cloud.bigtable.read_modify_write_rules import ReadModifyWriteRule
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.row import Row
from google.cloud.bigtable.row import _LastScannedRow
from google.cloud.bigtable.row_filters import RowFilter
from google.cloud.bigtable.sync import BigtableDataClient_Sync
from google.cloud.bigtable.sync import ReadRowsIterator_Sync
from google.cloud.bigtable.sync import Table_Sync
from google.cloud.bigtable.sync import _ReadRowsOperation_Sync
from google.cloud.bigtable_v2.services.bigtable.async_client import DEFAULT_CLIENT_INFO
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClient
from google.cloud.bigtable_v2.services.bigtable.transports.grpc import (
    BigtableGrpcTransport,
)
from google.cloud.bigtable_v2.types import RequestStats
from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.client import ClientWithProject
import google.auth._default
import google.auth.credentials


class _ReadRowsOperation_SyncAutoGenerated(Iterable[Row]):
    """
    ReadRowsOperation handles the logic of merging chunks from a ReadRowsResponse stream
    into a stream of Row objects.

    ReadRowsOperation.merge_row_response_stream takes in a stream of ReadRowsResponse
    and turns them into a stream of Row objects using an internal
    StateMachine.

    ReadRowsOperation(request, client) handles row merging logic end-to-end, including
    performing retries on stream errors.
    """

    def __init__(
        self,
        request: dict[str, Any],
        client: BigtableClient,
        *,
        buffer_size: int = 0,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ):
        """Args: - request: the request dict to send to the Bigtable API - client: the Bigtable client to use to make the request - buffer_size: the size of the buffer to use for caching rows from the network - operation_timeout: the timeout to use for the entire operation, in seconds - per_request_timeout: the timeout to use when waiting for each individual grpc request, in seconds"""
        self._last_seen_row_key: bytes | None = None
        self._emit_count = 0
        buffer_size = max(buffer_size, 0)
        self._request = request
        self.operation_timeout = operation_timeout
        row_limit = request.get("rows_limit", 0)
        self._partial_retryable = partial(
            self._read_rows_retryable_attempt,
            client.read_rows,
            buffer_size,
            per_request_timeout,
            row_limit,
        )
        predicate = retries.if_exception_type(
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            core_exceptions.Aborted,
        )

        def on_error_fn(exc):
            if predicate(exc):
                self.transient_errors.append(exc)

        retry = retries.Retry(
            predicate=predicate,
            timeout=self.operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
            on_error=on_error_fn,
            is_stream=True,
        )
        self._stream: Generator[Row | RequestStats, None, "Any"] | None = retry(
            self._partial_retryable
        )()
        self.transient_errors: List[Exception] = []

    def __iter__(self) -> Iterator[Row | RequestStats]:
        """Implements the Iterable interface"""
        return self

    def __next__(self) -> Row | RequestStats:
        """Implements the Iterator interface"""
        if self._stream is not None:
            return self._stream.__next__()
        else:
            raise GeneratorExit

    def close(self):
        """Close the stream and release resources"""
        if self._stream is not None:
            self._stream.close()
        self._stream = None
        self._last_seen_row_key = None

    def _read_rows_retryable_attempt(
        self,
        gapic_fn: Callable[..., Iterable[ReadRowsResponse]],
        buffer_size: int,
        per_request_timeout: float | None,
        total_row_limit: int,
    ) -> Generator[Row | RequestStats, None, "Any"]:
        """Retryable wrapper for merge_rows. This function is called each time a retry is attempted. Some fresh state is created on each retry: - grpc network stream - buffer for the stream - state machine to hold merge chunks received from stream Some state is shared between retries: - last_seen_row_key is used to ensure that duplicate rows are not emitted - request is stored and (optionally) modified on each retry"""
        if self._last_seen_row_key is not None:
            self._request["rows"] = self._revise_request_rowset(
                row_set=self._request.get("rows", None),
                last_seen_row_key=self._last_seen_row_key,
            )
            if total_row_limit:
                new_limit = total_row_limit - self._emit_count
                if new_limit <= 0:
                    return
                else:
                    self._request["rows_limit"] = new_limit
        params_str = f"table_name={self._request.get('table_name', '')}"
        if self._request.get("app_profile_id", None):
            params_str = (
                f"{params_str},app_profile_id={self._request.get('app_profile_id', '')}"
            )
        new_gapic_stream = gapic_fn(
            self._request,
            timeout=per_request_timeout,
            metadata=[("x-goog-request-params", params_str)],
        )
        (buffered_stream, buffer_task) = self._prepare_stream(
            new_gapic_stream, buffer_size
        )
        state_machine = _StateMachine()
        try:
            stream = self.merge_row_response_stream(buffered_stream, state_machine)
            while True:
                new_item = stream.__next__()
                if isinstance(new_item, Row) and (
                    self._last_seen_row_key is None
                    or new_item.row_key > self._last_seen_row_key
                ):
                    self._last_seen_row_key = new_item.row_key
                    if not isinstance(new_item, _LastScannedRow):
                        yield new_item
                        self._emit_count += 1
                        if total_row_limit and self._emit_count >= total_row_limit:
                            return
                elif isinstance(new_item, RequestStats):
                    yield new_item
        except StopIteration:
            return
        finally:
            if buffer_task is not None:
                buffer_task.cancel()

    @staticmethod
    def _prepare_stream(gapic_stream, buffer_size=None):
        raise NotImplementedError(
            "Corresponding Async Function contains unhandled asyncio calls"
        )

    @staticmethod
    def _revise_request_rowset(
        row_set: dict[str, Any] | None, last_seen_row_key: bytes
    ) -> dict[str, Any]:
        """Revise the rows in the request to avoid ones we've already processed. Args: - row_set: the row set from the request - last_seen_row_key: the last row key encountered"""
        if row_set is None or (
            len(row_set.get("row_ranges", [])) == 0
            and len(row_set.get("row_keys", [])) == 0
        ):
            last_seen = last_seen_row_key
            return {"row_keys": [], "row_ranges": [{"start_key_open": last_seen}]}
        else:
            row_keys: list[bytes] = row_set.get("row_keys", [])
            adjusted_keys = []
            for key in row_keys:
                if key > last_seen_row_key:
                    adjusted_keys.append(key)
            row_ranges: list[dict[str, Any]] = row_set.get("row_ranges", [])
            adjusted_ranges = []
            for row_range in row_ranges:
                end_key = row_range.get("end_key_closed", None) or row_range.get(
                    "end_key_open", None
                )
                if end_key is None or end_key > last_seen_row_key:
                    new_range = row_range.copy()
                    start_key = row_range.get(
                        "start_key_closed", None
                    ) or row_range.get("start_key_open", None)
                    if start_key is None or start_key <= last_seen_row_key:
                        new_range["start_key_open"] = last_seen_row_key
                        new_range.pop("start_key_closed", None)
                    adjusted_ranges.append(new_range)
            if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
                return row_set
            return {"row_keys": adjusted_keys, "row_ranges": adjusted_ranges}

    @staticmethod
    def merge_row_response_stream(
        request_generator: Iterable[ReadRowsResponse], state_machine: _StateMachine
    ) -> Generator[Row | RequestStats, None, "Any"]:
        """Consume chunks from a ReadRowsResponse stream into a set of Rows Args: - request_generator: Iterable of ReadRowsResponse objects. Typically this is a stream of chunks from the Bigtable API Returns: - Generator of Rows Raises: - InvalidChunk: if the chunk stream is invalid"""
        for row_response in request_generator:
            response_pb = row_response._pb
            last_scanned = response_pb.last_scanned_row_key
            if last_scanned:
                yield state_machine.handle_last_scanned_row(last_scanned)
            for chunk in response_pb.chunks:
                complete_row = state_machine.handle_chunk(chunk)
                if complete_row is not None:
                    yield complete_row
            if row_response.request_stats:
                yield row_response.request_stats
        if not state_machine.is_terminal_state():
            raise InvalidChunk("read_rows completed with partial state remaining")


class Table_SyncAutoGenerated:
    """
    Main Data API surface

    Table object maintains table_id, and app_profile_id context, and passes them with
    each call
    """

    def __init__(
        self,
        client: BigtableDataClient_Sync,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        *,
        default_operation_timeout: float = 60,
        default_per_request_timeout: float | None = None,
    ):
        """Initialize a Table_Sync instance Must be created within an async context (running event loop) Args: instance_id: The Bigtable instance ID to associate with this client. instance_id is combined with the client's project to fully specify the instance table_id: The ID of the table. table_id is combined with the instance_id and the client's project to fully specify the table app_profile_id: (Optional) The app profile to associate with requests. https://cloud.google.com/bigtable/docs/app-profiles default_operation_timeout: (Optional) The default timeout, in seconds default_per_request_timeout: (Optional) The default timeout for individual rpc requests, in seconds Raises: - RuntimeError if called outside of an async context (no running event loop)"""
        if default_operation_timeout <= 0:
            raise ValueError("default_operation_timeout must be greater than 0")
        if default_per_request_timeout is not None and default_per_request_timeout <= 0:
            raise ValueError("default_per_request_timeout must be greater than 0")
        if (
            default_per_request_timeout is not None
            and default_per_request_timeout > default_operation_timeout
        ):
            raise ValueError(
                "default_per_request_timeout must be less than default_operation_timeout"
            )
        self.client = client
        self.instance_id = instance_id
        self.instance_name = self.client._gapic_client.instance_path(
            self.client.project, instance_id
        )
        self.table_id = table_id
        self.table_name = self.client._gapic_client.table_path(
            self.client.project, instance_id, table_id
        )
        self.app_profile_id = app_profile_id
        self.default_operation_timeout = default_operation_timeout
        self.default_per_request_timeout = default_per_request_timeout
        self.__init__async__()

    def __init__async__(self):
        raise NotImplementedError(
            "Corresponding Async Function contains unhandled asyncio calls"
        )

    def read_rows_stream(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        buffer_size: int = 0,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ) -> ReadRowsIterator_Sync:
        """Returns an iterator to asynchronously stream back row data. Failed requests within operation_timeout and operation_deadline policies will be retried. By default, row data is streamed eagerly over the network, and fully bufferd in memory in the iterator, which can be consumed as needed. The size of the iterator buffer can be configured with buffer_size. When the buffer is full, the read_rows_stream will pause the network stream until space is available Args: - query: contains details about which rows to return - buffer_size: the number of rows to buffer in memory. If less than or equal to 0, buffer is unbounded. Defaults to 0 (unbounded) - operation_timeout: the time budget for the entire operation, in seconds. Failed requests will be retried within the budget. time is only counted while actively waiting on the network. Completed and bufferd results can still be accessed after the deadline is complete, with a DeadlineExceeded exception only raised after bufferd results are exhausted. If None, defaults to the Table's default_operation_timeout - per_request_timeout: the time budget for an individual network request, in seconds. If it takes longer than this time to complete, the request will be cancelled with a DeadlineExceeded exception, and a retry will be attempted. If None, defaults to the Table's default_per_request_timeout Returns: - an asynchronous iterator that yields rows returned by the query Raises: - DeadlineExceeded: raised after operation timeout will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions from any retries that failed - GoogleAPIError: raised if the request encounters an unrecoverable error - IdleTimeout: if iterator was abandoned"""
        operation_timeout = operation_timeout or self.default_operation_timeout
        per_request_timeout = per_request_timeout or self.default_per_request_timeout
        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout <= 0:
            raise ValueError("per_request_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout > operation_timeout:
            raise ValueError(
                "per_request_timeout must not be greater than operation_timeout"
            )
        if per_request_timeout is None:
            per_request_timeout = operation_timeout
        request = query._to_dict() if isinstance(query, ReadRowsQuery) else query
        request["table_name"] = self.table_name
        if self.app_profile_id:
            request["app_profile_id"] = self.app_profile_id
        row_merger = _ReadRowsOperation_Sync(
            request,
            self.client._gapic_client,
            buffer_size=buffer_size,
            operation_timeout=operation_timeout,
            per_request_timeout=per_request_timeout,
        )
        output_generator = ReadRowsIterator_Sync(row_merger)
        idle_timeout_seconds = 300
        output_generator._start_idle_timer(idle_timeout_seconds)
        return output_generator

    def read_rows(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ) -> list[Row]:
        """Helper function that returns a full list instead of a generator See read_rows_stream Returns: - a list of the rows returned by the query"""
        row_generator = self.read_rows_stream(
            query,
            operation_timeout=operation_timeout,
            per_request_timeout=per_request_timeout,
        )
        results = [row for row in row_generator]
        return results

    def read_row(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ) -> Row:
        """Helper function to return a single row See read_rows_stream Returns: - the individual row requested"""
        raise NotImplementedError

    def read_rows_sharded(
        self,
        query_list: list[ReadRowsQuery] | list[dict[str, Any]],
        *,
        limit: int | None,
        buffer_size: int | None = None,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ) -> ReadRowsIterator_Sync:
        """Runs a sharded query in parallel Each query in query list will be run concurrently, with results yielded as they are ready yielded results may be out of order Args: - query_list: a list of queries to run in parallel"""
        raise NotImplementedError

    def row_exists(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ) -> bool:
        """Helper function to determine if a row exists uses the filters: chain(limit cells per row = 1, strip value) Returns: - a bool indicating whether the row exists"""
        raise NotImplementedError

    def sample_keys(
        self,
        *,
        operation_timeout: int | float | None = 60,
        per_sample_timeout: int | float | None = 10,
        per_request_timeout: int | float | None = None,
    ) -> RowKeySamples:
        """Return a set of RowKeySamples that delimit contiguous sections of the table of approximately equal size RowKeySamples output can be used with ReadRowsQuery.shard() to create a sharded query that can be parallelized across multiple backend nodes read_rows and read_rows_stream requests will call sample_keys internally for this purpose when sharding is enabled RowKeySamples is simply a type alias for list[tuple[bytes, int]]; a list of row_keys, along with offset positions in the table Returns: - a set of RowKeySamples the delimit contiguous sections of the table Raises: - DeadlineExceeded: raised after operation timeout will be chained with a RetryExceptionGroup containing all GoogleAPIError exceptions from any retries that failed"""
        raise NotImplementedError

    def mutations_batcher(self, **kwargs) -> MutationsBatcher:
        """Implementation purposely removed in sync mode"""

    def mutate_row(
        self,
        row_key: str | bytes,
        mutations: list[Mutation] | Mutation,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ):
        """Mutates a row atomically. Cells already present in the row are left unchanged unless explicitly changed by ``mutation``. Idempotent operations (i.e, all mutations have an explicit timestamp) will be retried on server failure. Non-idempotent operations will not. Args: - row_key: the row to apply mutations to - mutations: the set of mutations to apply to the row - operation_timeout: the time budget for the entire operation, in seconds. Failed requests will be retried within the budget. time is only counted while actively waiting on the network. DeadlineExceeded exception raised after timeout - per_request_timeout: the time budget for an individual network request, in seconds. If it takes longer than this time to complete, the request will be cancelled with a DeadlineExceeded exception, and a retry will be attempted if within operation_timeout budget Raises: - DeadlineExceeded: raised after operation timeout will be chained with a RetryExceptionGroup containing all GoogleAPIError exceptions from any retries that failed - GoogleAPIError: raised on non-idempotent operations that cannot be safely retried."""
        raise NotImplementedError

    def bulk_mutate_rows(
        self,
        mutation_entries: list[BulkMutationsEntry],
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ):
        """Applies mutations for multiple rows in a single batched request. Each individual BulkMutationsEntry is applied atomically, but separate entries may be applied in arbitrary order (even for entries targetting the same row) In total, the row_mutations can contain at most 100000 individual mutations across all entries Idempotent entries (i.e., entries with mutations with explicit timestamps) will be retried on failure. Non-idempotent will not, and will reported in a raised exception group Args: - mutation_entries: the batches of mutations to apply Each entry will be applied atomically, but entries will be applied in arbitrary order - operation_timeout: the time budget for the entire operation, in seconds. Failed requests will be retried within the budget. time is only counted while actively waiting on the network. DeadlineExceeded exception raised after timeout - per_request_timeout: the time budget for an individual network request, in seconds. If it takes longer than this time to complete, the request will be cancelled with a DeadlineExceeded exception, and a retry will be attempted if within operation_timeout budget Raises: - MutationsExceptionGroup if one or more mutations fails Contains details about any failed entries in .exceptions"""
        raise NotImplementedError

    def check_and_mutate_row(
        self,
        row_key: str | bytes,
        predicate: RowFilter | None,
        true_case_mutations: Mutation | list[Mutation] | None = None,
        false_case_mutations: Mutation | list[Mutation] | None = None,
        operation_timeout: int | float | None = 60,
    ) -> bool:
        """Mutates a row atomically based on the output of a predicate filter Non-idempotent operation: will not be retried Args: - row_key: the key of the row to mutate - predicate: the filter to be applied to the contents of the specified row. Depending on whether or not any results are yielded, either true_case_mutations or false_case_mutations will be executed. If None, checks that the row contains any values at all. - true_case_mutations: Changes to be atomically applied to the specified row if predicate yields at least one cell when applied to row_key. Entries are applied in order, meaning that earlier mutations can be masked by later ones. Must contain at least one entry if false_case_mutations is empty, and at most 100000. - false_case_mutations: Changes to be atomically applied to the specified row if predicate_filter does not yield any cells when applied to row_key. Entries are applied in order, meaning that earlier mutations can be masked by later ones. Must contain at least one entry if `true_case_mutations is empty, and at most 100000. - operation_timeout: the time budget for the entire operation, in seconds. Failed requests will not be retried. Returns: - bool indicating whether the predicate was true or false Raises: - GoogleAPIError exceptions from grpc call"""
        raise NotImplementedError

    def read_modify_write_row(
        self,
        row_key: str | bytes,
        rules: ReadModifyWriteRule
        | list[ReadModifyWriteRule]
        | dict[str, Any]
        | list[dict[str, Any]],
        *,
        operation_timeout: int | float | None = 60,
    ) -> Row:
        """Reads and modifies a row atomically according to input ReadModifyWriteRules, and returns the contents of all modified cells The new value for the timestamp is the greater of the existing timestamp or the current server time. Non-idempotent operation: will not be retried Args: - row_key: the key of the row to apply read/modify/write rules to - rules: A rule or set of rules to apply to the row. Rules are applied in order, meaning that earlier rules will affect the results of later ones. - operation_timeout: the time budget for the entire operation, in seconds. Failed requests will not be retried. Returns: - Row: containing cell data that was modified as part of the operation Raises: - GoogleAPIError exceptions from grpc call"""
        raise NotImplementedError

    def close(self):
        """Called to close the Table_Sync instance and release any resources held by it."""
        self.client._remove_instance_registration(self.instance_id, self)

    def __enter__(self):
        """Implement async context manager protocol Register this instance with the client, so that grpc channels will be warmed for the specified instance"""
        self.client._register_instance(self.instance_id, self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Implement async context manager protocol Unregister this instance with the client, so that grpc channels will no longer be warmed"""
        self.close()


class BigtableDataClient_SyncAutoGenerated(ClientWithProject):
    def __init__(
        self,
        *,
        project: str | None = None,
        pool_size: int = 3,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
    ):
        """Create a client instance for the Bigtable Data API Client should be created within an async context (running event loop) Args: project: the project which the client acts on behalf of. If not passed, falls back to the default inferred from the environment. pool_size: The number of grpc channels to maintain in the internal channel pool. credentials: Thehe OAuth2 Credentials to use for this client. If not passed (and if no ``_http`` object is passed), falls back to the default inferred from the environment. client_options (Optional[Union[dict, google.api_core.client_options.ClientOptions]]): Client options used to set user options on the client. API Endpoint should be set through client_options. Raises: - RuntimeError if called outside of an async context (no running event loop) - ValueError if pool_size is less than 1"""
        client_info = DEFAULT_CLIENT_INFO
        client_info.client_library_version = client_info.gapic_version
        if type(client_options) is dict:
            client_options = client_options_lib.from_dict(client_options)
        client_options = cast(
            Optional[client_options_lib.ClientOptions], client_options
        )
        ClientWithProject.__init__(
            self,
            credentials=credentials,
            project=project,
            client_options=client_options,
        )
        transport_str = self.__init__transport__(pool_size)
        self._gapic_client = BigtableClient(
            transport=transport_str,
            credentials=credentials,
            client_options=client_options,
            client_info=client_info,
        )
        self.transport = cast(BigtableGrpcTransport, self._gapic_client.transport)
        self._active_instances: Set[str] = set()
        self._instance_owners: dict[str, Set[int]] = {}
        self._channel_init_time = time.time()
        self._channel_refresh_tasks: list[asyncio.Task[None]] = []
        try:
            self.start_background_channel_refresh()
        except RuntimeError:
            warnings.warn(
                f"{self.__class__.__name__} should be started in an asyncio event loop. Channel refresh will not be started",
                RuntimeWarning,
                stacklevel=2,
            )

    def __init__transport__(self, pool_size: int):
        """Implementation purposely removed in sync mode"""

    def start_background_channel_refresh(self) -> None:
        """Implementation purposely removed in sync mode"""

    def close(self, timeout: float = 2.0):
        raise NotImplementedError(
            "Corresponding Async Function contains unhandled asyncio calls"
        )

    def _ping_and_warm_instances(
        self, channel: grpc.aio.Channel
    ) -> list[GoogleAPICallError | None]:
        raise NotImplementedError(
            "Corresponding Async Function contains unhandled asyncio calls"
        )

    def _manage_channel(
        self,
        channel_idx: int,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        grace_period: float = 60 * 10,
    ) -> None:
        """Implementation purposely removed in sync mode"""

    def _register_instance(self, instance_id: str, owner: Table_Sync) -> None:
        """Implementation purposely removed in sync mode"""

    def _remove_instance_registration(
        self, instance_id: str, owner: Table_Sync
    ) -> bool:
        """Removes an instance from the client's registered instances, to prevent warming new channels for the instance If instance_id is not registered, or is still in use by other tables, returns False Args: - instance_id: id of the instance to remove - owner: table that owns the instance. Owners will be tracked in _instance_owners, and instances will only be unregistered when all owners call _remove_instance_registration Returns: - True if instance was removed"""
        instance_name = self._gapic_client.instance_path(self.project, instance_id)
        owner_list = self._instance_owners.get(instance_name, set())
        try:
            owner_list.remove(id(owner))
            if len(owner_list) == 0:
                self._active_instances.remove(instance_name)
            return True
        except KeyError:
            return False

    def get_table(
        self,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        default_operation_timeout: float = 60,
        default_per_request_timeout: float | None = None,
    ) -> Table_Sync:
        """Returns a table instance for making data API requests Args: instance_id: The Bigtable instance ID to associate with this client. instance_id is combined with the client's project to fully specify the instance table_id: The ID of the table. app_profile_id: (Optional) The app profile to associate with requests. https://cloud.google.com/bigtable/docs/app-profiles"""
        return Table_Sync(
            self,
            instance_id,
            table_id,
            app_profile_id,
            default_operation_timeout=default_operation_timeout,
            default_per_request_timeout=default_per_request_timeout,
        )

    def __enter__(self):
        self.start_background_channel_refresh()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._gapic_client.__exit__(exc_type, exc_val, exc_tb)


class ReadRowsIterator_SyncAutoGenerated(Iterable[Row]):
    """
    Async iterator for ReadRows responses.
    """

    def __init__(self, merger: _ReadRowsOperation_Sync):
        self._merger: _ReadRowsOperation_Sync = merger
        self._error: Exception | None = None
        self.request_stats: RequestStats | None = None
        self.last_interaction_time = time.time()
        self._idle_timeout_task: asyncio.Task[None] | None = None

    def _start_idle_timer(self, idle_timeout: float):
        """Implementation purposely removed in sync mode"""

    @property
    def active(self):
        """Returns True if the iterator is still active and has not been closed"""
        return self._error is None

    def __iter__(self):
        """Implement the Iterator protocol."""
        return self

    def __next__(self) -> Row:
        """Implement the Iterator potocol. Return the next item in the stream if active, or raise an exception if the stream has been closed."""
        if self._error is not None:
            raise self._error
        try:
            self.last_interaction_time = time.time()
            next_item = self._merger.__next__()
            if isinstance(next_item, RequestStats):
                self.request_stats = next_item
                return self.__next__()
            else:
                return next_item
        except core_exceptions.RetryError:
            new_exc = core_exceptions.DeadlineExceeded(
                f"operation_timeout of {self._merger.operation_timeout:0.1f}s exceeded"
            )
            source_exc = None
            if self._merger.transient_errors:
                source_exc = RetryExceptionGroup(
                    f"{len(self._merger.transient_errors)} failed attempts",
                    self._merger.transient_errors,
                )
            new_exc.__cause__ = source_exc
            self._finish_with_error(new_exc)
            raise new_exc from source_exc
        except Exception as e:
            self._finish_with_error(e)
            raise e

    def _finish_with_error(self, e: Exception):
        """Helper function to close the stream and clean up resources after an error has occurred."""
        if self.active:
            self._merger.close()
            self._error = e
        if self._idle_timeout_task is not None:
            self._idle_timeout_task.cancel()
            self._idle_timeout_task = None

    def close(self):
        """Support closing the stream with an explicit call to aclose()"""
        self._finish_with_error(StopIteration(f"{self.__class__.__name__} closed"))
