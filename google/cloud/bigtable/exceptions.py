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

import sys

from typing import Any, TYPE_CHECKING

from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.row import Row

is_311_plus = sys.version_info >= (3, 11)

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import RowMutationEntry
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery


class IdleTimeout(core_exceptions.DeadlineExceeded):
    """
    Exception raised by ReadRowsIterator when the generator
    has been idle for longer than the internal idle_timeout.
    """

    pass


class InvalidChunk(core_exceptions.GoogleAPICallError):
    """Exception raised to invalid chunk data from back-end."""


class _RowSetComplete(Exception):
    """
    Internal exception for _ReadRowsOperation
    Raised in revise_request_rowset when there are no rows left to process when starting a retry attempt
    """

    pass


class BigtableExceptionGroup(ExceptionGroup if is_311_plus else Exception):  # type: ignore # noqa: F821
    """
    Represents one or more exceptions that occur during a bulk Bigtable operation

    In Python 3.11+, this is an unmodified exception group. In < 3.10, it is a
    custom exception with some exception group functionality backported, but does
    Not implement the full API
    """

    def __init__(self, message, excs):
        if is_311_plus:
            super().__init__(message, excs)
        else:
            if len(excs) == 0:
                raise ValueError("exceptions must be a non-empty sequence")
            self.exceptions = tuple(excs)
            super().__init__(message)

    def __new__(cls, message, excs):
        if is_311_plus:
            return super().__new__(cls, message, excs)
        else:
            return super().__new__(cls)

    def __str__(self):
        """
        String representation doesn't display sub-exceptions. Subexceptions are
        described in message
        """
        return self.args[0]


class MutationsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a bulk mutation operation
    """

    @staticmethod
    def _format_message(excs: list[FailedMutationEntryError], total_entries: int):
        entry_str = "entry" if total_entries == 1 else "entries"
        plural_str = "" if len(excs) == 1 else "s"
        return f"{len(excs)} sub-exception{plural_str} (from {total_entries} {entry_str} attempted)"

    def __init__(self, excs: list[FailedMutationEntryError], total_entries: int):
        super().__init__(self._format_message(excs, total_entries), excs)

    def __new__(cls, excs: list[FailedMutationEntryError], total_entries: int):
        return super().__new__(cls, cls._format_message(excs, total_entries), excs)


class FailedMutationEntryError(Exception):
    """
    Represents a single failed RowMutationEntry in a bulk_mutate_rows request.
    A collection of FailedMutationEntryErrors will be raised in a MutationsExceptionGroup
    """

    def __init__(
        self,
        failed_idx: int,
        failed_mutation_entry: "RowMutationEntry",
        cause: Exception,
    ):
        idempotent_msg = (
            "idempotent" if failed_mutation_entry.is_idempotent() else "non-idempotent"
        )
        message = f"Failed {idempotent_msg} mutation entry at index {failed_idx} with cause: {cause!r}"
        super().__init__(message)
        self.index = failed_idx
        self.entry = failed_mutation_entry
        self.__cause__ = cause


class RetryExceptionGroup(BigtableExceptionGroup):
    """Represents one or more exceptions that occur during a retryable operation"""

    @staticmethod
    def _format_message(excs: list[Exception]):
        if len(excs) == 0:
            return "No exceptions"
        if len(excs) == 1:
            return f"1 failed attempt: {type(excs[0]).__name__}"
        else:
            return f"{len(excs)} failed attempts. Latest: {type(excs[-1]).__name__}"

    def __init__(self, excs: list[Exception]):
        super().__init__(self._format_message(excs), excs)

    def __new__(cls, excs: list[Exception]):
        return super().__new__(cls, cls._format_message(excs), excs)


class ShardedReadRowsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a sharded read rows operation
    """

    @staticmethod
    def _format_message(excs: list[FailedQueryShardError], total_queries: int):
        query_str = "query" if total_queries == 1 else "queries"
        plural_str = "" if len(excs) == 1 else "s"
        return f"{len(excs)} sub-exception{plural_str} (from {total_queries} {query_str} attempted)"

    def __init__(
        self,
        excs: list[FailedQueryShardError],
        succeeded: list[Row],
        total_queries: int,
    ):
        super().__init__(self._format_message(excs, total_queries), excs)
        self.successful_rows = succeeded

    def __new__(
        cls, excs: list[FailedQueryShardError], succeeded: list[Row], total_queries: int
    ):
        instance = super().__new__(cls, cls._format_message(excs, total_queries), excs)
        instance.successful_rows = succeeded
        return instance


class FailedQueryShardError(Exception):
    """
    Represents an individual failed query in a sharded read rows operation
    """

    def __init__(
        self,
        failed_index: int,
        failed_query: "ReadRowsQuery" | dict[str, Any],
        cause: Exception,
    ):
        message = f"Failed query at index {failed_index} with cause: {cause!r}"
        super().__init__(message)
        self.index = failed_index
        self.query = failed_query
        self.__cause__ = cause
