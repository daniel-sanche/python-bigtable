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

from typing import Deque

# java implementation: 
# https://github.com/googleapis/java-bigtable/blob/8b120de58f0dfba3573ab696fb0e5375e917a00e/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/readrows/RowMerger.java

class RowMerger():

    def __init__(self):
        self.merged_rows:Deque[Row] = deque([])
        self.state_machine = StateMachine()

    def push(self, new_data:ReadRowsResponse):
        last_scanned = new_data.last_scanned_row_key
        # if the server sends a scan heartbeat, notify the state machine.
        if last_scanned is not None:
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
        return not self.merged_rows

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

    def handle_last_scanned_row(self, last_scanned_row_key:bytes):
        pass

    def handle_chunk(self, chunk:ReadRowsResponse.CellChunk):
        pass

    def has_complete_row(self) -> bool:
        return False

    def consume_row(self) -> Row:
        pass

    def is_row_in_progress(self) -> bool:
        return True
