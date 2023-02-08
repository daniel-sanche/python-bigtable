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

from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.bigtable.row import Row, DirectRow, InvalidChunk, PartialRowData, Cell
from google.protobuf.wrappers_pb2 import StringValue, BytesValue
from collections import deque, namedtuple
from datetime import datetime

from typing import Deque, Optional, List, Dict, Set, Any

# java implementation:
# https://github.com/googleapis/java-bigtable/blob/8b120de58f0dfba3573ab696fb0e5375e917a00e/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/readrows/RowMerger.java


class RowMerger:
    def __init__(self):
        self.merged_rows: Deque[PartialRowData] = deque([])
        self.state_machine = StateMachine()

    def push(self, new_data: ReadRowsResponse):
        last_scanned = new_data.last_scanned_row_key
        # if the server sends a scan heartbeat, notify the state machine.
        if last_scanned:
            self.state_machine.handle_last_scanned_row(last_scanned)
            if self.state_machine.has_complete_row():
                self.merged_rows.append(self.state_machine.consume_row())
        # process new chunks through the state machine.
        for chunk in new_data.chunks:
            self.state_machine.handle_chunk(chunk)
            if self.state_machine.has_complete_row():
                self.merged_rows.append(self.state_machine.consume_row())

    def has_full_frame(self) -> bool:
        """
        one or more rows are ready and waiting to be consumed
        """
        return bool(self.merged_rows)

    def has_partial_frame(self) -> bool:
        """
        Returns true if the merger still has ongoing state
        By the end of the process, there should be no partial state
        """
        return self.has_full_frame() or self.state_machine.is_row_in_progress()

    def pop(self) -> PartialRowData:
        """
        Return a row out of the cache of waiting rows
        """
        return self.merged_rows.popleft()


class StateMachine:
    def __init__(self):
        self.completed_row_keys:Set[bytes] = set({})
        self.adapter: "RowBuilder" = RowBuilder()
        self.reset()

    def reset(self):
        self.current_state: Optional[State] = AWAITING_NEW_ROW(self)
        self.last_cell_data: Dict[str, Any] = {}
        # represents either the last row emitted, or the last_scanned_key sent from backend
        # all future rows should have keys > last_seen_row_key
        self.last_seen_row_key:Optional[bytes] = None
        # self.expected_cell_size:int = 0
        # self.remaining_cell_bytes:int = 0
        self.complete_row: Optional[PartialRowData] = None
        # self.num_cells_in_row:int = 0
        self.adapter.reset()

    def handle_last_scanned_row(self, last_scanned_row_key: bytes):
        if self.last_seen_row_key and self.last_seen_row_key >= last_scanned_row_key:
            raise InvalidChunk("Last scanned row key is out of order")
        self.last_scanned_row_key = last_scanned_row_key
        assert isinstance(self.current_state, State)
        self.current_state = self.current_state.handle_last_scanned_row(
            last_scanned_row_key
        )

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk):
        assert isinstance(self.current_state, State)
        if chunk.row_key in self.completed_row_keys:
            raise InvalidChunk(f"duplicate row key: {chunk.row_key}")
        self.current_state = self.current_state.handle_chunk(chunk)

    def has_complete_row(self) -> bool:
        return (
            isinstance(self.current_state, AWAITING_ROW_CONSUME)
            and self.complete_row is not None
        )

    def consume_row(self) -> PartialRowData:
        """
        Returns the last completed row and transitions to a new row
        """
        if not self.has_complete_row() or self.complete_row is None:
            raise RuntimeError("No row to consume")
        row = self.complete_row
        self.reset()
        self.completed_row_keys.add(row.row_key)
        return row

    def is_row_in_progress(self) -> bool:
        return not isinstance(self.current_state, AWAITING_NEW_ROW)

    def handle_commit_row(self) -> "State":
        """
        Called when a row is complete.
        Wait in AWAITING_ROW_CONSUME state for the RowMerger to consume it
        """
        self.complete_row = self.adapter.finish_row()
        self.last_seen_row_key = self.complete_row.row_key
        return AWAITING_ROW_CONSUME(self)

    def handle_reset_chunk(
        self, chunk: ReadRowsResponse.CellChunk
    ) -> "AWAITING_NEW_ROW":
        """
        When a reset chunk comes in, drop all buffers and reset to AWAITING_NEW_ROW state
        """
        # ensure reset chunk matches expectations
        if isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("Bare reset")
        if chunk.row_key:
            raise InvalidChunk("Reset chunk has a row key")
        if chunk.HasField("family_name"):
            raise InvalidChunk("Reset chunk has family_name")
        if chunk.HasField("qualifier"):
            raise InvalidChunk("Reset chunk has qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("Reset chunk has a timestamp")
        if chunk.labels:
            raise InvalidChunk("Reset chunk has labels")
        if chunk.value:
            raise InvalidChunk("Reset chunk has a value")
        self.reset()
        assert isinstance(self.current_state, AWAITING_NEW_ROW)
        return self.current_state


class State:
    def __init__(self, owner: "StateMachine"):
        self._owner = owner

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> "State":
        raise NotImplementedError

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        raise NotImplementedError


class AWAITING_NEW_ROW(State):
    """
    Default state
    Awaiting a chunk to start a new row

    Exit states: any (depending on chunk)
    """

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> "State":
        self._owner.complete_row = self._owner.adapter.create_scan_marker_row(
            last_scanned_row_key
        )
        return AWAITING_ROW_CONSUME(self._owner)

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
        if self._owner.last_seen_row_key and self._owner.last_seen_row_key >= chunk.row_key.value:
            raise InvalidChunk("Out of order row keys")
        self._owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL(self._owner).handle_chunk(chunk)


class AWAITING_NEW_CELL(State):
    """
    Represents a cell boundary witin a row

    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        if chunk.reset_row:
            return self._owner.handle_reset_chunk(chunk)
        chunk_size = len(chunk.value)
        is_split = chunk.value_size > 0
        expected_cell_size = chunk.value_size if is_split else chunk_size
        # unwrap proto values if needed
        family_name = chunk.family_name.value if isinstance(chunk.family_name, StringValue) else chunk.family_name
        qualifier = chunk.qualifier.value if isinstance(chunk.qualifier, BytesValue) else chunk.qualifier
        # track latest cell data. New chunks won't send repeated data
        if family_name:
            self._owner.last_cell_data["family"] = family_name
            if not qualifier:
                raise InvalidChunk("new column family must specify qualifier")
        if qualifier:
            self._owner.last_cell_data["qualifier"] = qualifier
            if not self._owner.last_cell_data.get("family", False):
                raise InvalidChunk("family not found")
        self._owner.last_cell_data["labels"] = chunk.labels
        self._owner.last_cell_data["timestamp"] = chunk.timestamp_micros

        # ensure that all chunks after the first one either are missing a row
        # key or the row is the same
        if self._owner.adapter.row_in_progress() and chunk.row_key and chunk.row_key != self._owner.adapter.current_key:
            raise InvalidChunk("row key changed mid row")

        self._owner.adapter.start_cell(
            **self._owner.last_cell_data, size=expected_cell_size
        )
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if is_split:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            if chunk.commit_row:
                # row is also complete
                return self._owner.handle_commit_row()
            else:
                # wait for more cells for this row
                return AWAITING_NEW_CELL(self._owner)


class AWAITING_CELL_VALUE(State):
    """
    State that represents a split cell's continuation

    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        # ensure reset chunk matches expectations
        if chunk.row_key:
            raise InvalidChunk("found row key mid cell")
        if chunk.HasField("family_name"):
            raise InvalidChunk("In progress cell had a family name")
        if chunk.HasField("qualifier"):
            raise InvalidChunk("In progress cell had a qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("In progress cell had a timestamp")
        if chunk.labels:
            raise InvalidChunk("In progress cell had labels")
        # check for reset row
        if chunk.reset_row:
            return self._owner.handle_reset_chunk(chunk)
        is_last = chunk.value_size == 0
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if not is_last:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            if chunk.commit_row:
                # row is also complete
                return self._owner.handle_commit_row()
            else:
                # wait for more cells for this row
                return AWAITING_NEW_CELL(self._owner)


class AWAITING_ROW_CONSUME(State):
    """
    Represents a completed row. Prevents new rows being read until it is consumed

    Exit states: AWAITING_NEW_ROW
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        raise RuntimeError("Skipping completed row")


CellData = namedtuple(
    "CellData", ["family", "qualifier", "timestamp", "labels", "value"]
)


class RowBuilder:
    """
    called by state machine to build rows

    State machine makes the following guarantees:
        Exactly 1 `start_row` for each row.
        Exactly 1 `start_cell` for each cell.
        At least 1 `cell_value` for each cell.
        Exactly 1 `finish_cell` for each cell.
        Exactly 1 `finish_row` for each row.
    `create_scan_marker_row` can be called one or more times between `finish_row` and
    `start_row`. `reset` can be called at any point and can be invoked multiple times in
    a row.
    """

    def __init__(self):
        self.reset()

    def row_in_progress(self) -> bool:
        return self.current_key is not None

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key: Optional[bytes] = None
        self.working_cell: Optional[CellData] = None
        self.previous_cells: List[CellData] = []

    def create_scan_marker_row(self, key: bytes) -> PartialRowData:
        """creates a special row to mark server progress before any data is received"""
        return PartialRowData(key)

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        self.current_key = key

    def start_cell(
        self,
        family: str,
        qualifier: bytes,
        timestamp: int,
        labels: List[str],
        size: int,
    ) -> None:
        """called to start a new cell in a row."""
        if not family:
            raise InvalidChunk("missing family for a new cell")
        if qualifier is None:
            raise InvalidChunk("missing qualifier for a new cell")
        self.working_cell = CellData(family, qualifier, timestamp, labels, bytearray())

    def cell_value(self, value: bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        assert isinstance(self.working_cell, CellData)
        self.working_cell.value.extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        assert isinstance(self.working_cell, CellData)
        self.previous_cells.append(self.working_cell)
        self.working_cell = None

    def finish_row(self) -> PartialRowData:
        """called once per row to signal that all cells have been processed (unless reset)"""
        cell_data = {}
        for cell in self.previous_cells:
            # TODO: handle timezones?
            # should probably make a new row class
            # timestamp = datetime.fromtimestamp(cell.timestamp / 1e6)
            family_dict = cell_data.get(cell.family, {})
            qualifier_arr = family_dict.get(cell.qualifier, [])
            qualifier_arr.append(Cell(bytes(cell.value), cell.timestamp, cell.labels))
            family_dict[cell.qualifier] = qualifier_arr
            cell_data[cell.family] = family_dict
        new_row = PartialRowData(self.current_key)
        new_row._cells = cell_data
        self.reset()
        return new_row
