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

from dataclasses import dataclass
from google.cloud.bigtable.row_response import family_id, qualifier, row_key


class Mutation:
    pass


@dataclass
class SetCell(Mutation):
    family: family_id
    qualifier: qualifier
    new_value: bytes | str | int
    timestamp_ms: int | None = None


@dataclass
class DeleteRangeFromColumn(Mutation):
    family: family_id
    qualifier: qualifier
    start_timestamp_ms: int
    end_timestamp_ms: int


@dataclass
class DeleteAllFromFamily(Mutation):
    family_to_delete: family_id


@dataclass
class DeleteAllFromRow(Mutation):
    pass


@dataclass
class BulkMutationsEntry:
    row_key: row_key
    mutations: list[Mutation] | Mutation
