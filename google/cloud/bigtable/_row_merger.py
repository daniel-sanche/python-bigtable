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
from __future__ import annotations

from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.types import RequestStats
from google.cloud.bigtable.row import Row, Cell, _LastScannedRow
from google.cloud.bigtable.exceptions import InvalidChunk
import asyncio
from functools import partial
from google.api_core import retry_async as retries
from google.api_core import exceptions as core_exceptions

from abc import ABC, abstractmethod

from typing import (
    cast,
    List,
    Any,
    AsyncIterable,
    AsyncIterator,
    AsyncGenerator,
    Callable,
    Awaitable,
)

"""
This module provides a set of classes for merging ReadRowsResponse chunks
into Row objects.

- RowMerger is the highest level class, providing an interface for asynchronous
  merging end-to-end
- StateMachine is used internally to track the state of the merge, including
  the current row key and the keys of the rows that have been processed.
  It processes a stream of chunks, and will raise InvalidChunk if it reaches
  an invalid state.
- State classes track the current state of the StateMachine, and define what
  to do on the next chunk.
- RowBuilder is used by the StateMachine to build a Row object.
"""


class _RowMerger(AsyncIterable[Row]):
    """
    RowMerger handles the logic of merging chunks from a ReadRowsResponse stream
    into a stream of Row objects.

    RowMerger.merge_row_response_stream takes in a stream of ReadRowsResponse
    and turns them into a stream of Row objects using an internal
    StateMachine.

    RowMerger(request, client) handles row merging logic end-to-end, including
    performing retries on stream errors.
    """

    def __init__(
        self,
        request: dict[str, Any],
        client: BigtableAsyncClient,
        *,
        buffer_size: int = 0,
        operation_timeout: float | None = None,
        per_row_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ):
        """
        Args:
          - request: the request dict to send to the Bigtable API
          - client: the Bigtable client to use to make the request
          - buffer_size: the size of the buffer to use for caching rows from the network
          - operation_timeout: the timeout to use for the entire operation, in seconds
          - per_row_timeout: the timeout to use when waiting for each individual row, in seconds
          - per_request_timeout: the timeout to use when waiting for each individual grpc request, in seconds
        """
        self.last_seen_row_key: bytes | None = None
        self.emit_count = 0
        buffer_size = max(buffer_size, 0)
        self.request = request
        self.operation_timeout = operation_timeout
        row_limit = request.get("rows_limit", 0)
        # lock in paramters for retryable wrapper
        self.partial_retryable = partial(
            self.retryable_merge_rows,
            client.read_rows,
            buffer_size,
            per_row_timeout,
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
                self.errors.append(exc)

        retry = retries.AsyncRetry(
            predicate=predicate,
            timeout=self.operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
            on_error=on_error_fn,
            is_generator=True,
        )
        self.stream: AsyncGenerator[Row | RequestStats, None] | None = retry(
            self.partial_retryable
        )()
        self.errors: List[Exception] = []

    def __aiter__(self) -> AsyncIterator[Row | RequestStats]:
        """Implements the AsyncIterable interface"""
        return self

    async def __anext__(self) -> Row | RequestStats:
        """Implements the AsyncIterator interface"""
        if isinstance(self.stream, AsyncGenerator):
            return await self.stream.__anext__()
        else:
            raise asyncio.InvalidStateError("stream is closed")

    async def aclose(self):
        """Close the stream and release resources"""
        if isinstance(self.stream, AsyncGenerator):
            await self.stream.aclose()
        del self.stream
        self.stream = None
        self.last_seen_row_key = None

    @staticmethod
    async def _generator_to_buffer(
        buffer: asyncio.Queue[Any], input_generator: AsyncIterable[Any]
    ) -> None:
        """
        Helper function to push items from an async generator into a buffer
        """
        async for item in input_generator:
            await buffer.put(item)

    async def retryable_merge_rows(
        self,
        gapic_fn: Callable[..., Awaitable[AsyncIterable[ReadRowsResponse]]],
        buffer_size: int,
        per_row_timeout: float | None,
        per_request_timeout: float | None,
        row_limit: int,
    ) -> AsyncGenerator[Row | RequestStats, None]:
        """
        Retryable wrapper for merge_rows. This function is called each time
        a retry is attempted.

        Some fresh state is created on each retry:
          - grpc network stream
          - buffer for the stream
          - state machine to hold merge chunks received from stream
        Some state is shared between retries:
          - last_seen_row_key is used to ensure that
            duplicate rows are not emitted
          - request is stored and (optionally) modified on each retry
        """
        if self.last_seen_row_key is not None:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            self.request["rows"] = _RowMerger._revise_request_rowset(
                row_set=self.request.get("rows", None),
                last_seen_row_key=self.last_seen_row_key,
            )
            # revise next request's row limit based on number emitted
            if row_limit:
                new_limit = row_limit - self.emit_count
                if new_limit <= 0:
                    return
                else:
                    self.request["rows_limit"] = new_limit
        new_gapic_stream = await gapic_fn(
            self.request,
            timeout=per_request_timeout,
        )
        buffer: asyncio.Queue[Row | RequestStats] = asyncio.Queue(maxsize=buffer_size)
        state_machine = _StateMachine()
        try:
            stream_task = asyncio.create_task(
                _RowMerger._generator_to_buffer(
                    buffer,
                    _RowMerger.merge_row_response_stream(
                        new_gapic_stream, state_machine
                    ),
                )
            )
            get_from_buffer_task = asyncio.create_task(buffer.get())
            # sleep to allow other tasks to run
            await asyncio.sleep(0)
            # read from state machine and push into buffer
            # when finished, stream will be done, buffer will be empty, but get_from_buffer_task will still be waiting
            while (
                not stream_task.done()
                or not buffer.empty()
                or get_from_buffer_task.done()
            ):
                if get_from_buffer_task.done():
                    new_item = get_from_buffer_task.result()
                    # don't yield rows that have already been emitted
                    if isinstance(new_item, RequestStats):
                        yield new_item
                    # ignore rows that have already been emitted
                    elif isinstance(new_item, Row) and (
                        self.last_seen_row_key is None
                        or new_item.row_key > self.last_seen_row_key
                    ):
                        self.last_seen_row_key = new_item.row_key
                        # don't yeild _LastScannedRow markers; they
                        # should only update last_seen_row_key
                        if not isinstance(new_item, _LastScannedRow):
                            yield new_item
                            self.emit_count += 1
                            if row_limit and self.emit_count >= row_limit:
                                return
                    # start new task for buffer
                    get_from_buffer_task = asyncio.create_task(buffer.get())
                    await asyncio.sleep(0)
                else:
                    # wait for either the stream to finish, or a new item to enter the buffer
                    first_finish = asyncio.wait(
                        [stream_task, get_from_buffer_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    await asyncio.wait_for(first_finish, per_row_timeout)
            # stream and buffer are complete. if there's an exception, raise it
            if stream_task.exception():
                raise cast(Exception, stream_task.exception())
        except asyncio.TimeoutError:
            # per_row_timeout from asyncio.wait_for
            raise core_exceptions.DeadlineExceeded(
                f"per_row_timeout of {per_row_timeout:0.1f}s exceeded"
            )
        finally:
            stream_task.cancel()
            get_from_buffer_task.cancel()

    @staticmethod
    def _revise_request_rowset(
        row_set: dict[str, Any] | None,
        last_seen_row_key: bytes,
    ) -> dict[str, Any]:
        """
        Revise the rows in the request to avoid ones we've already processed.

        Args:
          - row_set: the row set from the request
          - last_seen_row_key: the last row key encountered
        """
        # if user is doing a whole table scan, start a new one with the last seen key
        if row_set is None:
            last_seen = last_seen_row_key
            return {
                "row_keys": [],
                "row_ranges": [{"start_key_open": last_seen}],
            }
        else:
            # remove seen keys from user-specific key list
            row_keys: list[bytes] = row_set.get("row_keys", [])
            adjusted_keys = []
            for key in row_keys:
                if key > last_seen_row_key:
                    adjusted_keys.append(key)
            # if user specified only a single range, set start to the last seen key
            row_ranges: list[dict[str, Any]] = row_set.get("row_ranges", [])
            if len(row_keys) == 0 and len(row_ranges) == 1:
                row_ranges[0]["start_key_open"] = last_seen_row_key
                if "start_key_closed" in row_ranges[0]:
                    row_ranges[0].pop("start_key_closed")
            return {"row_keys": adjusted_keys, "row_ranges": row_ranges}

    @staticmethod
    async def merge_row_response_stream(
        request_generator: AsyncIterable[ReadRowsResponse], state_machine: _StateMachine
    ) -> AsyncGenerator[Row | RequestStats, None]:
        """
        Consume chunks from a ReadRowsResponse stream into a set of Rows

        Args:
          - request_generator: AsyncIterable of ReadRowsResponse objects. Typically
                this is a stream of chunks from the Bigtable API
        Returns:
            - AsyncGenerator of Rows
        Raises:
            - InvalidChunk: if the chunk stream is invalid
        """
        async for row_response in request_generator:
            # unwrap protoplus object for increased performance
            response_pb = row_response._pb
            last_scanned = response_pb.last_scanned_row_key
            # if the server sends a scan heartbeat, notify the state machine.
            if last_scanned:
                yield state_machine.handle_last_scanned_row(last_scanned)
            # process new chunks through the state machine.
            for chunk in response_pb.chunks:
                complete_row = state_machine.handle_chunk(chunk)
                if complete_row is not None:
                    yield complete_row
            # yield request stats if present
            if row_response.request_stats:
                yield row_response.request_stats
        if not state_machine.is_terminal_state():
            # read rows is complete, but there's still data in the merger
            raise InvalidChunk("read_rows completed with partial state remaining")


class _StateMachine:
    """
    State Machine converts chunks into Rows

    Chunks are added to the state machine via handle_chunk, which
    transitions the state machine through the various states.

    When a row is complete, it will be returned from handle_chunk,
    and the state machine will reset to AWAITING_NEW_ROW

    If an unexpected chunk is received for the current state,
    the state machine will raise an InvalidChunk exception
    """

    __slots__ = (
        "current_state",
        "current_family",
        "current_qualifier",
        "last_seen_row_key",
        "adapter",
    )

    def __init__(self):
        # represents either the last row emitted, or the last_scanned_key sent from backend
        # all future rows should have keys > last_seen_row_key
        self.last_seen_row_key: bytes | None = None
        self.adapter = _RowBuilder()
        self._reset_row()

    def _reset_row(self) -> None:
        """
        Drops the current row and transitions to AWAITING_NEW_ROW to start a fresh one
        """
        self.current_state: _State = AWAITING_NEW_ROW(self)
        self.current_family: str | None = None
        self.current_qualifier: bytes | None = None
        # self.expected_cell_size:int = 0
        # self.remaining_cell_bytes:int = 0
        # self.num_cells_in_row:int = 0
        self.adapter.reset()

    def is_terminal_state(self) -> bool:
        """
        Returns true if the state machine is in a terminal state (AWAITING_NEW_ROW)

        At the end of the read_rows stream, if the state machine is not in a terminal
        state, an exception should be raised
        """
        return isinstance(self.current_state, AWAITING_NEW_ROW)

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> Row:
        """
        Called by RowMerger to notify the state machine of a scan heartbeat

        Returns an empty row with the last_scanned_row_key
        """
        if self.last_seen_row_key and self.last_seen_row_key >= last_scanned_row_key:
            raise InvalidChunk("Last scanned row key is out of order")
        if not isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("Last scanned row key received in invalid state")
        scan_marker = _LastScannedRow(last_scanned_row_key)
        self._handle_complete_row(scan_marker)
        return scan_marker

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> Row | None:
        """
        Called by RowMerger to process a new chunk

        Returns a Row if the chunk completes a row, otherwise returns None
        """
        if (
            self.last_seen_row_key
            and chunk.row_key
            and self.last_seen_row_key >= chunk.row_key
        ):
            raise InvalidChunk("row keys should be strictly increasing")
        if chunk.reset_row:
            # reset row if requested
            self._handle_reset_chunk(chunk)
        else:
            # otherwise, process the chunk and update the state
            self.current_state = self.current_state.handle_chunk(chunk)
        if chunk.commit_row:
            # check if row is complete, and return it if so
            if not isinstance(self.current_state, AWAITING_NEW_CELL):
                raise InvalidChunk("commit row attempted without finishing cell")
            complete_row = self.adapter.finish_row()
            self._handle_complete_row(complete_row)
            return complete_row
        else:
            # row is not complete, return None
            return None

    def _handle_complete_row(self, complete_row: Row) -> None:
        """
        Complete row, update seen keys, and move back to AWAITING_NEW_ROW

        Called by StateMachine when a commit_row flag is set on a chunk,
        or when a scan heartbeat is received
        """
        self.last_seen_row_key = complete_row.row_key
        self._reset_row()

    def _handle_reset_chunk(self, chunk: ReadRowsResponse.CellChunk):
        """
        Drop all buffers and reset the row in progress

        Called by StateMachine when a reset_row flag is set on a chunk
        """
        # ensure reset chunk matches expectations
        if isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("Reset chunk received when not processing row")
        if chunk.row_key:
            raise InvalidChunk("Reset chunk has a row key")
        if chunk.family_name.value:
            raise InvalidChunk("Reset chunk has family_name")
        if chunk.qualifier.value:
            raise InvalidChunk("Reset chunk has qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("Reset chunk has a timestamp")
        if chunk.labels:
            raise InvalidChunk("Reset chunk has labels")
        if chunk.value:
            raise InvalidChunk("Reset chunk has a value")
        self._reset_row()


class _State(ABC):
    """
    Represents a state the state machine can be in

    Each state is responsible for handling the next chunk, and then
    transitioning to the next state
    """

    __slots__ = ("_owner",)

    def __init__(self, owner: _StateMachine):
        self._owner = owner

    @abstractmethod
    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "_State":
        pass


class AWAITING_NEW_ROW(_State):
    """
    Default state
    Awaiting a chunk to start a new row
    Exit states:
      - AWAITING_NEW_CELL: when a chunk with a row_key is received
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "_State":
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
        self._owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL(self._owner).handle_chunk(chunk)


class AWAITING_NEW_CELL(_State):
    """
    Represents a cell boundary witin a row

    Exit states:
    - AWAITING_NEW_CELL: when the incoming cell is complete and ready for another
    - AWAITING_CELL_VALUE: when the value is split across multiple chunks
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "_State":
        is_split = chunk.value_size > 0
        # expected_cell_size = chunk.value_size if is_split else len(chunk.value)
        # track latest cell data. New chunks won't send repeated data
        if chunk.family_name.value:
            self._owner.current_family = chunk.family_name.value
            if not chunk.qualifier.value:
                raise InvalidChunk("New column family must specify qualifier")
        if chunk.qualifier.value:
            self._owner.current_qualifier = chunk.qualifier.value
            if self._owner.current_family is None:
                raise InvalidChunk("Family not found")

        # ensure that all chunks after the first one are either missing a row
        # key or the row is the same
        if chunk.row_key and chunk.row_key != self._owner.adapter.current_key:
            raise InvalidChunk("Row key changed mid row")
        self._owner.adapter.start_cell(
            family=self._owner.current_family,
            qualifier=self._owner.current_qualifier,
            labels=list(chunk.labels),
            timestamp_micros=chunk.timestamp_micros,
        )
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if is_split:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            return AWAITING_NEW_CELL(self._owner)


class AWAITING_CELL_VALUE(_State):
    """
    State that represents a split cell's continuation

    Exit states:
    - AWAITING_NEW_CELL: when the cell is complete
    - AWAITING_CELL_VALUE: when additional value chunks are required
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "_State":
        # ensure reset chunk matches expectations
        if chunk.row_key:
            raise InvalidChunk("Found row key mid cell")
        if chunk.family_name.value:
            raise InvalidChunk("In progress cell had a family name")
        if chunk.qualifier.value:
            raise InvalidChunk("In progress cell had a qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("In progress cell had a timestamp")
        if chunk.labels:
            raise InvalidChunk("In progress cell had labels")
        is_last = chunk.value_size == 0
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if not is_last:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            return AWAITING_NEW_CELL(self._owner)


class _RowBuilder:
    """
    called by state machine to build rows
    State machine makes the following guarantees:
        Exactly 1 `start_row` for each row.
        Exactly 1 `start_cell` for each cell.
        At least 1 `cell_value` for each cell.
        Exactly 1 `finish_cell` for each cell.
        Exactly 1 `finish_row` for each row.
    `reset` can be called at any point and can be invoked multiple times in
    a row.
    """

    __slots__ = "current_key", "working_cell", "working_value", "completed_cells"

    def __init__(self):
        # initialize state
        self.reset()

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key: bytes | None = None
        self.working_cell: Cell | None = None
        self.working_value: bytearray | None = None
        self.completed_cells: List[Cell] = []

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        if (
            self.current_key is not None
            or self.working_cell is not None
            or self.working_value is not None
            or self.completed_cells
        ):
            raise InvalidChunk("start_row called without finishing previous row")
        self.current_key = key

    def start_cell(
        self,
        family: str | None,
        qualifier: bytes | None,
        timestamp_micros: int,
        labels: List[str],
    ) -> None:
        """called to start a new cell in a row."""
        if not family:
            raise InvalidChunk("Missing family for a new cell")
        if qualifier is None:
            raise InvalidChunk("Missing qualifier for a new cell")
        if self.current_key is None:
            raise InvalidChunk("start_cell called without a row")
        self.working_value = bytearray()
        self.working_cell = Cell(
            b"", self.current_key, family, qualifier, timestamp_micros, labels
        )

    def cell_value(self, value: bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        if self.working_value is None:
            raise InvalidChunk("Cell value received before start_cell")
        self.working_value.extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        if self.working_cell is None or self.working_value is None:
            raise InvalidChunk("Cell value received before start_cell")
        self.working_cell.value = bytes(self.working_value)
        self.completed_cells.append(self.working_cell)
        self.working_cell = None
        self.working_value = None

    def finish_row(self) -> Row:
        """called once per row to signal that all cells have been processed (unless reset)"""
        if self.current_key is None:
            raise InvalidChunk("No row in progress")
        new_row = Row(self.current_key, self.completed_cells)
        self.reset()
        return new_row
