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
# This file is automatically generated by CrossSync. Do not edit manually.
from __future__ import annotations
from typing import TYPE_CHECKING, Awaitable, Sequence
from google.cloud.bigtable_v2.types import ReadRowsRequest as ReadRowsRequestPB
from google.cloud.bigtable_v2.types import ReadRowsResponse as ReadRowsResponsePB
from google.cloud.bigtable_v2.types import RowSet as RowSetPB
from google.cloud.bigtable_v2.types import RowRange as RowRangePB
from google.cloud.bigtable.data.row import Row, Cell
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import _RowSetComplete
from google.cloud.bigtable.data import _helpers
from google.api_core import retry as retries
from google.api_core.retry import exponential_sleep_generator
from google.cloud.bigtable.data._sync.cross_sync import CrossSync

if TYPE_CHECKING:
    if CrossSync._Sync_Impl.is_async:
        from google.cloud.bigtable.data._async.client import TableAsync
    else:
        from typing import Iterable


class _ReadRowsOperation:
    """
    ReadRowsOperation handles the logic of merging chunks from a ReadRowsResponse stream
    into a stream of Row objects.

    ReadRowsOperation.merge_row_response_stream takes in a stream of ReadRowsResponse
    and turns them into a stream of Row objects using an internal
    StateMachine.

    ReadRowsOperation(request, client) handles row merging logic end-to-end, including
    performing retries on stream errors.

    Args:
        query: The query to execute
        table: The table to send the request to
        operation_timeout: The total time to allow for the operation, in seconds
        attempt_timeout: The time to allow for each individual attempt, in seconds
        retryable_exceptions: A list of exceptions that should trigger a retry
    """

    __slots__ = (
        "attempt_timeout_gen",
        "operation_timeout",
        "request",
        "table",
        "_predicate",
        "_metadata",
        "_last_yielded_row_key",
        "_remaining_count",
    )

    def __init__(
        self,
        query: ReadRowsQuery,
        table: "Table",
        operation_timeout: float,
        attempt_timeout: float,
        retryable_exceptions: Sequence[type[Exception]] = (),
    ):
        self.attempt_timeout_gen = _helpers._attempt_timeout_generator(
            attempt_timeout, operation_timeout
        )
        self.operation_timeout = operation_timeout
        if isinstance(query, dict):
            self.request = ReadRowsRequestPB(
                **query,
                table_name=table.table_name,
                app_profile_id=table.app_profile_id,
            )
        else:
            self.request = query._to_pb(table)
        self.table = table
        self._predicate = retries.if_exception_type(*retryable_exceptions)
        self._metadata = _helpers._make_metadata(table.table_name, table.app_profile_id)
        self._last_yielded_row_key: bytes | None = None
        self._remaining_count: int | None = self.request.rows_limit or None

    def start_operation(self) -> Iterable[Row]:
        """Start the read_rows operation, retrying on retryable errors.

        Yields:
            Row: The next row in the stream"""
        return CrossSync._Sync_Impl.retry_target_stream(
            self._read_rows_attempt,
            self._predicate,
            exponential_sleep_generator(0.01, 60, multiplier=2),
            self.operation_timeout,
            exception_factory=_helpers._retry_exception_factory,
        )

    def _read_rows_attempt(self) -> Iterable[Row]:
        """Attempt a single read_rows rpc call.
        This function is intended to be wrapped by retry logic,
        which will call this function until it succeeds or
        a non-retryable error is raised.

        Yields:
            Row: The next row in the stream"""
        if self._last_yielded_row_key is not None:
            try:
                self.request.rows = self._revise_request_rowset(
                    row_set=self.request.rows,
                    last_seen_row_key=self._last_yielded_row_key,
                )
            except _RowSetComplete:
                return self.merge_rows(None)
        if self._remaining_count is not None:
            self.request.rows_limit = self._remaining_count
            if self._remaining_count == 0:
                return self.merge_rows(None)
        gapic_stream = self.table.client._gapic_client.read_rows(
            self.request,
            timeout=next(self.attempt_timeout_gen),
            metadata=self._metadata,
            retry=None,
        )
        chunked_stream = self.chunk_stream(gapic_stream)
        return self.merge_rows(chunked_stream)

    def chunk_stream(
        self, stream: Iterable[ReadRowsResponsePB]
    ) -> Iterable[ReadRowsResponsePB.CellChunk]:
        """process chunks out of raw read_rows stream

        Args:
            stream: the raw read_rows stream from the gapic client
        Yields:
            ReadRowsResponsePB.CellChunk: the next chunk in the stream"""
        for resp in stream:
            resp = resp._pb
            if resp.last_scanned_row_key:
                if (
                    self._last_yielded_row_key is not None
                    and resp.last_scanned_row_key <= self._last_yielded_row_key
                ):
                    raise InvalidChunk("last scanned out of order")
                self._last_yielded_row_key = resp.last_scanned_row_key
            current_key = None
            for c in resp.chunks:
                if current_key is None:
                    current_key = c.row_key
                    if current_key is None:
                        raise InvalidChunk("first chunk is missing a row key")
                    elif (
                        self._last_yielded_row_key
                        and current_key <= self._last_yielded_row_key
                    ):
                        raise InvalidChunk("row keys should be strictly increasing")
                yield c
                if c.reset_row:
                    current_key = None
                elif c.commit_row:
                    self._last_yielded_row_key = current_key
                    if self._remaining_count is not None:
                        self._remaining_count -= 1
                        if self._remaining_count < 0:
                            raise InvalidChunk("emit count exceeds row limit")
                    current_key = None

    @staticmethod
    def merge_rows(
        chunks: Iterable[ReadRowsResponsePB.CellChunk] | None,
    ) -> Iterable[Row]:
        """Merge chunks into rows

        Args:
            chunks: the chunk stream to merge
        Yields:
            Row: the next row in the stream"""
        if chunks is None:
            return
        it = chunks.__iter__()
        while True:
            try:
                c = it.__next__()
            except StopIteration:
                return
            row_key = c.row_key
            if not row_key:
                raise InvalidChunk("first row chunk is missing key")
            cells = []
            family: str | None = None
            qualifier: bytes | None = None
            try:
                while True:
                    if c.reset_row:
                        raise _ResetRow(c)
                    k = c.row_key
                    f = c.family_name.value
                    q = c.qualifier.value if c.HasField("qualifier") else None
                    if k and k != row_key:
                        raise InvalidChunk("unexpected new row key")
                    if f:
                        family = f
                        if q is not None:
                            qualifier = q
                        else:
                            raise InvalidChunk("new family without qualifier")
                    elif family is None:
                        raise InvalidChunk("missing family")
                    elif q is not None:
                        if family is None:
                            raise InvalidChunk("new qualifier without family")
                        qualifier = q
                    elif qualifier is None:
                        raise InvalidChunk("missing qualifier")
                    ts = c.timestamp_micros
                    labels = c.labels if c.labels else []
                    value = c.value
                    if c.value_size > 0:
                        buffer = [value]
                        while c.value_size > 0:
                            c = it.__next__()
                            t = c.timestamp_micros
                            cl = c.labels
                            k = c.row_key
                            if (
                                c.HasField("family_name")
                                and c.family_name.value != family
                            ):
                                raise InvalidChunk("family changed mid cell")
                            if (
                                c.HasField("qualifier")
                                and c.qualifier.value != qualifier
                            ):
                                raise InvalidChunk("qualifier changed mid cell")
                            if t and t != ts:
                                raise InvalidChunk("timestamp changed mid cell")
                            if cl and cl != labels:
                                raise InvalidChunk("labels changed mid cell")
                            if k and k != row_key:
                                raise InvalidChunk("row key changed mid cell")
                            if c.reset_row:
                                raise _ResetRow(c)
                            buffer.append(c.value)
                        value = b"".join(buffer)
                    cells.append(
                        Cell(value, row_key, family, qualifier, ts, list(labels))
                    )
                    if c.commit_row:
                        yield Row(row_key, cells)
                        break
                    c = it.__next__()
            except _ResetRow as e:
                c = e.chunk
                if (
                    c.row_key
                    or c.HasField("family_name")
                    or c.HasField("qualifier")
                    or c.timestamp_micros
                    or c.labels
                    or c.value
                ):
                    raise InvalidChunk("reset row with data")
                continue
            except StopIteration:
                raise InvalidChunk("premature end of stream")

    @staticmethod
    def _revise_request_rowset(row_set: RowSetPB, last_seen_row_key: bytes) -> RowSetPB:
        """Revise the rows in the request to avoid ones we've already processed.

        Args:
            row_set: the row set from the request
            last_seen_row_key: the last row key encountered
        Returns:
            RowSetPB: the new rowset after adusting for the last seen key
        Raises:
            _RowSetComplete: if there are no rows left to process after the revision"""
        if row_set is None or (not row_set.row_ranges and (not row_set.row_keys)):
            last_seen = last_seen_row_key
            return RowSetPB(row_ranges=[RowRangePB(start_key_open=last_seen)])
        adjusted_keys: list[bytes] = [
            k for k in row_set.row_keys if k > last_seen_row_key
        ]
        adjusted_ranges: list[RowRangePB] = []
        for row_range in row_set.row_ranges:
            end_key = row_range.end_key_closed or row_range.end_key_open or None
            if end_key is None or end_key > last_seen_row_key:
                new_range = RowRangePB(row_range)
                start_key = row_range.start_key_closed or row_range.start_key_open
                if start_key is None or start_key <= last_seen_row_key:
                    new_range.start_key_open = last_seen_row_key
                adjusted_ranges.append(new_range)
        if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
            raise _RowSetComplete()
        return RowSetPB(row_keys=adjusted_keys, row_ranges=adjusted_ranges)
