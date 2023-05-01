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
from typing import Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from sys import getsizeof


class Mutation(ABC):
    """Model class for mutations"""

    @abstractmethod
    def _to_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    def is_idempotent(self) -> bool:
        """
        Check if the mutation is idempotent
        If false, the mutation will not be retried
        """
        return True

    def __str__(self) -> str:
        return str(self._to_dict())

    def size(self) -> int:
        """
        Get the size of the mutation in bytes
        """
        return getsizeof(self._to_dict())


@dataclass
class SetCell(Mutation):
    family: str
    qualifier: bytes
    new_value: bytes
    timestamp_micros: int | None = None

    def _to_dict(self) -> dict[str, Any]:
        """Convert the mutation to a dictionary representation"""
        # if timestamp not given, use -1 for server-side timestamp
        timestamp = self.timestamp_micros if self.timestamp_micros is not None else -1
        return {
            "set_cell": {
                "family_name": self.family,
                "column_qualifier": self.qualifier,
                "timestamp_micros": timestamp,
                "value": self.new_value,
            }
        }

    def is_idempotent(self) -> bool:
        """Check if the mutation is idempotent"""
        return self.timestamp_micros is not None and self.timestamp_micros >= 0


@dataclass
class DeleteRangeFromColumn(Mutation):
    family: str
    qualifier: bytes
    # None represents 0
    start_timestamp_micros: int | None = None
    # None represents infinity
    end_timestamp_micros: int | None = None

    def __post_init__(self):
        if (
            self.start_timestamp_micros is not None
            and self.end_timestamp_micros is not None
            and self.start_timestamp_micros > self.end_timestamp_micros
        ):
            raise ValueError("start_timestamp_micros must be <= end_timestamp_micros")

    def _to_dict(self) -> dict[str, Any]:
        timestamp_range = {}
        if self.start_timestamp_micros is not None:
            timestamp_range["start_timestamp_micros"] = self.start_timestamp_micros
        if self.end_timestamp_micros is not None:
            timestamp_range["end_timestamp_micros"] = self.end_timestamp_micros
        return {
            "delete_from_column": {
                "family_name": self.family,
                "column_qualifier": self.qualifier,
                "time_range": timestamp_range,
            }
        }


@dataclass
class DeleteAllFromFamily(Mutation):
    family_to_delete: str

    def _to_dict(self) -> dict[str, Any]:
        return {
            "delete_from_family": {
                "family_name": self.family_to_delete,
            }
        }


@dataclass
class DeleteAllFromRow(Mutation):
    def _to_dict(self) -> dict[str, Any]:
        return {
            "delete_from_row": {},
        }


class BulkMutationsEntry:
    def __init__(self, row_key: bytes | str, mutations: Mutation | list[Mutation]):
        if isinstance(row_key, str):
            row_key = row_key.encode("utf-8")
        if isinstance(mutations, Mutation):
            mutations = [mutations]
        self.row_key = row_key
        self.mutations = mutations

    def _to_dict(self) -> dict[str, Any]:
        return {
            "row_key": self.row_key,
            "mutations": [mutation._to_dict() for mutation in self.mutations],
        }

    def is_idempotent(self) -> bool:
        """Check if the mutation is idempotent"""
        return all(mutation.is_idempotent() for mutation in self.mutations)

    def size(self) -> int:
        """
        Get the size of the mutation in bytes
        """
        return getsizeof(self._to_dict())
