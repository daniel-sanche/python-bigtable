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
from google.cloud.bigtable.row import Row, DirectRow
from collections import deque, namedtuple
from datetime import datetime

from typing import Deque, Optional, List, Dict, Any

# java implementation:
# https://github.com/googleapis/java-bigtable/blob/8b120de58f0dfba3573ab696fb0e5375e917a00e/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/readrows/RowMerger.java

class RowMerger():

    def __init__(self):
        self.merged_rows:Deque[Row] = deque([])
        self.state_machine = StateMachine()

    def push(self, new_data:ReadRowsResponse):
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
        return self.merged_rows

    def has_partial_frame(self) -> bool:
        """
        Returns true if the merger still has ongoing state
        By the end of the process, there should be no partial state
        """
        return self.has_full_frame() or self.state_machine.is_row_in_progress()

    def pop(self) -> Row:
        """
        Return a row out of the cache of waiting rows
        """
        return self.merged_rows.popleft()


class StateMachine():

    def __init__(self):
        self.adapter:"RowBuilder" = RowBuilder()
        self.reset()

    def reset(self):
        self.current_state:Optional[str] = AWAITING_NEW_ROW(self)
        self.last_cell_data:Dict[str, Any] = {}
        # self.last_complete_row_key:Optional[bytes] = None
        # self.row_key:Optional[bytes] = None
        # self.family_name:Optional[str] = None
        # self.qualifier:Optional[str] = None
        # self.timestamp:int = 0
        # self.labels:List[str] = None
        # self.expected_cell_size:int = 0
        # self.remaining_cell_bytes:int = 0
        self.complete_row:Optional[Row] = None
        # self.num_cells_in_row:int = 0
        self.adapter.reset()

    def handle_last_scanned_row(self, last_scanned_row_key:bytes):
        pass

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk):
        self.current_state = self.current_state.handle_chunk(chunk);

    def has_complete_row(self) -> bool:
        return isinstance(self.current_state, AWAITING_ROW_CONSUME) and self.complete_row is not None

    def consume_row(self) -> Row:
        """
        Returns the last completed row and transitions to a new row
        """
        if not self.has_complete_row() or self.complete_row is None:
            raise RuntimeError("No row to consume")
        row = self.complete_row
        self.reset()
        return row

    def is_row_in_progress(self) -> bool:
        return isinstance(self.current_state, AWAITING_NEW_ROW)

    def handle_commit_row(self) -> "State":
        """
        Called when a row is complete.
        Wait in AWAITING_ROW_CONSUME state for the RowMerger to consume it
        """
        self.complete_row = self.adapter.finish_row()
        # self.last_complete_row_key = self.complete_row.key
        return AWAITING_ROW_CONSUME(self)


    def handle_reset_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "AWAITING_NEW_ROW":
        """
        When a reset chunk comes in, drop all buffers and reset to AWAITING_NEW_ROW state
        """
        self.reset()
        assert isinstance(self.current_state, AWAITING_NEW_ROW)
        return self.current_state

class State:

    def __init__(self, owner:"StateMachine"):
        self._owner = owner

    def handle_last_scanned_row(self, last_scanned_row_key:bytes) -> "State":
        raise NotImplementedError

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
        raise NotImplementedError

class AWAITING_NEW_ROW(State):
    """
    Default state
    Awaiting a chunk to start a new row

    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
        self._owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL(self._owner).handle_chunk(chunk)

class AWAITING_NEW_CELL(State):
    """
    Represents a cell boundary witin a row

    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
        if chunk.reset_row:
            return self._owner.handle_reset_chunk(chunk)
        chunk_size = len(chunk.value)
        is_split = chunk.value_size > 0
        expected_cell_size = chunk.value_size if is_split else chunk_size

        # track latest cell data. New chunks won't send repeated data
        if chunk.family_name:
            self._owner.last_cell_data["family"] = chunk.family_name
        if chunk.qualifier:
            self._owner.last_cell_data["qualifier"] = chunk.qualifier
        self._owner.last_cell_data["labels"] = chunk.labels
        self._owner.last_cell_data["timestamp"] = chunk.timestamp_micros

        self._owner.adapter.start_cell(**self._owner.last_cell_data, size=expected_cell_size)
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
    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
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
    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
        raise RuntimeError("Skipping completed row")

CellData = namedtuple('CellData', ['family', 'qualifier', 'timestamp', 'labels', 'value'])

class RowBuilder():
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

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key:Optional[bytes] = None
        self.working_cell:Optional[CellData] = None
        self.previous_cells:List[CellData] = []

    def create_scan_marker_row(self, key:bytes) -> Row:
        """creates a special row to mark server progress before any data is received"""
        return Row(key)

    def start_row(self, key:bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        self.current_key = key

    def start_cell(self, family:str, qualifier:bytes, timestamp:int,labels:List[str], size:int) -> None:
        """called to start a new cell in a row."""
        self.working_cell = CellData(family, qualifier, timestamp, labels, bytearray())

    def cell_value(self, value:bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        self.working_cell.value.extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        self.previous_cells.append(self.working_cell)
        self.working_cell = None

    def finish_row(self) -> Row:
        """called once per row to signal that all cells have been processed (unless reset)"""
        new_row = DirectRow(self.current_key)
        for cell in self.previous_cells:
            # TODO: handle timezones?
            # should probably make a new row class
            timestamp = datetime.fromtimestamp(cell.timestamp/1e6)
            new_row.set_cell(cell.family, cell.qualifier, bytes(cell.value), timestamp)
        self.previous_cells.clear()
        self.current_key = None
        return new_row


