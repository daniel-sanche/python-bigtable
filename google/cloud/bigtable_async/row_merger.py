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
from google.cloud.bigtable.row import Row
from collections import deque

from typing import Deque, Optional, List

import inspect
# java implementation: 
# https://github.com/googleapis/java-bigtable/blob/8b120de58f0dfba3573ab696fb0e5375e917a00e/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/readrows/RowMerger.java

class RowMerger():

    def __init__(self):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        self.merged_rows:Deque[Row] = deque([])
        self.state_machine = StateMachine()

    def push(self, new_data:ReadRowsResponse):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
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
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return self.merged_rows

    def has_partial_frame(self) -> bool:
        """
        Returns true if the merger still has ongoing state
        By the end of the process, there should be no partial state
        """
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return self.has_full_frame() or self.state_machine.is_row_in_progress()

    def pop(self) -> Row:
        """
        Return a row out of the cache of waiting rows
        """
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return self.merged_rows.popleft()


class StateMachine():

    current_state:Optional[str] = "AWAITING_NEW_ROW"
    row_key:Optional[str] = None
    family_name:Optional[str] = None
    qualifier:Optional[str] = None
    timestamp:int = 0
    labels:List[str] = None
    expected_cell_size:int = 0
    remaining_cell_bytes:int = 0
    complete_row:Optional[Row] = None
    num_cells_in_row:int = 0

    def handle_last_scanned_row(self, last_scanned_row_key:bytes):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        pass

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        pass

    def has_complete_row(self) -> bool:
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return True

    def consume_row(self) -> Row:
        """
        Returns the last completed row and transitions to a new row
        """
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return Row(b"")

    def is_row_in_progress(self) -> bool:
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return True

    def reset(self):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        self.current_state = "AWAITING_NEW_ROW"
        self.row_key = None
        self.family_name = None
        self.qualifier = None
        self.timestamp = 0
        self.labels = None
        self.expected_cell_size = 0
        self.remaining_cell_bytes = 0
        self.complete_row = None
        self.num_cells_in_row = 0

class State:
    def handle_last_scanned_row(self, last_scanned_row_key:bytes) -> "State":
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        raise NotImplementedError

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk) -> "State":
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        raise NotImplementedError

class AWAITING_NEW_ROW(State):
    """
    Default state
    Awaiting a chunk to start a new row

    Exit states: any (depending on chunk)
    """
    pass

class AWAITING_NEW_CELL(State):
    """
    Represents a cell boundary witin a row

    Exit states: any (depending on chunk)
    """
    pass

class AWAITING_CELL_VALUE(State):
    """
    State that represents a cell's continuation

    Exit states: any (depending on chunk)
    """
    pass

class AWAITING_ROW_CONSUME(State):
    """
    Represents a completed row. Prevents new eos being read until it is consumed

    Exit states: AWAITING_NEW_ROW
    """
    pass

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
    def start_row(self, key:bytes) -> None:
       """Called to start a new row. This will be called once per row"""
       print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
       return

    def start_cell(self, family:str, qualifier:bytes, timestamp:int,labels:List[str], size:int) -> None:
        """called to start a new cell in a row."""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return

    def cell_value(self, value:bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return

    def finish_row(self) -> Row:
        """called once per row to signal that all cells have been processed (unless reset)"""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return Row(b"")

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return

    def create_scan_marker_row(self, key:bytes) -> Row:
        """creates a special row to mark server progress before any data is received"""
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return Row(key)
