# -*- coding: utf-8 -*-
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

from typing import List, Tuple

from google.cloud.bigtable import gapic_version as package_version

from google.cloud.bigtable.data._async.client import BigtableDataClientAsync
from google.cloud.bigtable.data._async.client import TableAsync

from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data.read_rows_query import RowRange
from google.cloud.bigtable.data.row import Row
from google.cloud.bigtable.data.row import Cell

from google.cloud.bigtable.data._async.mutations_batcher import MutationsBatcherAsync
from google.cloud.bigtable.data.mutations import Mutation
from google.cloud.bigtable.data.mutations import RowMutationEntry
from google.cloud.bigtable.data.mutations import SetCell
from google.cloud.bigtable.data.mutations import DeleteRangeFromColumn
from google.cloud.bigtable.data.mutations import DeleteAllFromFamily
from google.cloud.bigtable.data.mutations import DeleteAllFromRow

# Type alias for the output of sample_keys
RowKeySamples = List[Tuple[bytes, int]]
# type alias for the output of query.shard()
ShardedQuery = List[ReadRowsQuery]

__version__: str = package_version.__version__

__all__ = (
    "BigtableDataClientAsync",
    "TableAsync",
    "RowKeySamples",
    "ReadRowsQuery",
    "RowRange",
    "MutationsBatcherAsync",
    "Mutation",
    "RowMutationEntry",
    "SetCell",
    "DeleteRangeFromColumn",
    "DeleteAllFromFamily",
    "DeleteAllFromRow",
    "Row",
    "Cell",
)
