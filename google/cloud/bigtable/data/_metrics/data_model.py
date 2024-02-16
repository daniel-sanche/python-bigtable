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
from __future__ import annotations

from typing import Callable, Any, Generator, Tuple, cast, TYPE_CHECKING

import datetime
import time
import os
import re
import logging

from enum import Enum
from functools import lru_cache
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions
from google.cloud.bigtable_v2.types.response_params import ResponseParams
from google.protobuf.message import DecodeError

if TYPE_CHECKING:
    from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler


LOGGER = logging.getLogger(__name__)

# default values for zone and cluster data, if not captured
DEFAULT_ZONE = "global"
DEFAULT_CLUSTER_ID = "unspecified"

# keys for parsing metadata blobs
BIGTABLE_METADATA_KEY = "x-goog-ext-425905942-bin"
SERVER_TIMING_METADATA_KEY = "server-timing"
SERVER_TIMING_REGEX = re.compile(r".*gfet4t7;\s*dur=(\d+\.?\d*).*")

INVALID_STATE_ERROR = "Invalid state for {}: {}"


@dataclass(frozen=True)
class TimeTuple:
    """
    Tuple that holds both the utc timestamp to record with the metrics, and a
    monotonic timestamp for calculating durations. The monotonic timestamp is
    preferred for calculations because it is resilient to clock changes, eg DST
    """

    utc: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    monotonic: float = field(default_factory=time.monotonic)


class OperationType(Enum):
    """Enum for the type of operation being performed."""

    READ_ROWS = "ReadRows"
    SAMPLE_ROW_KEYS = "SampleRowKeys"
    BULK_MUTATE_ROWS = "MutateRows"
    MUTATE_ROW = "MutateRow"
    CHECK_AND_MUTATE = "CheckAndMutateRow"
    READ_MODIFY_WRITE = "ReadModifyWriteRow"


class OperationState(Enum):
    """Enum for the state of the active operation."""

    CREATED = 0
    ACTIVE_ATTEMPT = 1
    BETWEEN_ATTEMPTS = 2
    COMPLETED = 3


@dataclass(frozen=True)
class CompletedAttemptMetric:
    """
    An immutable dataclass representing the data associated with a
    completed rpc attempt.

    Operation-level fields (eg. type, cluster, zone) are stored on the
    corresponding CompletedOperationMetric or ActiveOperationMetric object.
    """

    start_time: datetime.datetime
    duration_ms: float
    end_status: StatusCode
    first_response_latency_ms: float | None = None
    gfe_latency_ms: float | None = None
    application_blocking_time_ms: float = 0.0
    backoff_before_attempt_ms: float = 0.0
    grpc_throttling_time_ms: float = 0.0


@dataclass(frozen=True)
class CompletedOperationMetric:
    """
    An immutable dataclass representing the data associated with a
    completed rpc operation.

    Attempt-level fields (eg. duration, latencies, etc) are stored on the
    corresponding CompletedAttemptMetric object.
    """

    op_type: OperationType
    start_time: datetime.datetime
    duration_ms: float
    completed_attempts: list[CompletedAttemptMetric]
    final_status: StatusCode
    cluster_id: str
    zone: str
    is_streaming: bool
    flow_throttling_time_ms: float = 0.0


@dataclass
class ActiveAttemptMetric:
    """
    A dataclass representing the data associated with an rpc attempt that is
    currently in progress. Fields are mutable and may be optional.
    """

    # keep both clock time and monotonic timestamps for active attempts
    start_time: TimeTuple = field(default_factory=TimeTuple)
    # the time it takes to recieve the first response from the server, in milliseconds
    # currently only tracked for ReadRows
    first_response_latency_ms: float | None = None
    # the time taken by the backend, in milliseconds. Taken from response header
    gfe_latency_ms: float | None = None
    # time waiting on user to process the response, in milliseconds
    # currently only relevant for ReadRows
    application_blocking_time_ms: float = 0.0
    # backoff time is added to application_blocking_time_ms
    backoff_before_attempt_ms: float = 0.0
    # time waiting on grpc channel, in milliseconds
    # TODO: capture grpc_throttling_time
    grpc_throttling_time_ms: float = 0.0


@dataclass
class ActiveOperationMetric:
    """
    A dataclass representing the data associated with an rpc operation that is
    currently in progress. Fields are mutable and may be optional.
    """

    op_type: OperationType
    backoff_generator: Generator[float, int, None] | None = None
    # keep both clock time and monotonic timestamps for active operations
    start_time: TimeTuple = field(default_factory=TimeTuple)
    active_attempt: ActiveAttemptMetric | None = None
    cluster_id: str | None = None
    zone: str | None = None
    completed_attempts: list[CompletedAttemptMetric] = field(default_factory=list)
    is_streaming: bool = False  # only True for read_rows operations
    was_completed: bool = False
    handlers: list[MetricsHandler] = field(default_factory=list)
    # time waiting on flow control, in milliseconds
    flow_throttling_time_ms: float = 0.0

    @property
    def state(self) -> OperationState:
        if self.was_completed:
            return OperationState.COMPLETED
        elif self.active_attempt is None:
            if self.completed_attempts:
                return OperationState.BETWEEN_ATTEMPTS
            else:
                return OperationState.CREATED
        else:
            return OperationState.ACTIVE_ATTEMPT

    def start(self) -> None:
        """
        Optionally called to mark the start of the operation. If not called,
        the operation will be started at initialization.

        Assunes operation is in CREATED state.
        """
        if self.state != OperationState.CREATED:
            return self._handle_error(INVALID_STATE_ERROR.format("start", self.state))
        self.start_time = TimeTuple()

    def start_attempt(self) -> None:
        """
        Called to initiate a new attempt for the operation.

        Assumes operation is in either CREATED or BETWEEN_ATTEMPTS states
        """
        if (
            self.state != OperationState.BETWEEN_ATTEMPTS
            and self.state != OperationState.CREATED
        ):
            return self._handle_error(
                INVALID_STATE_ERROR.format("start_attempt", self.state)
            )

        # find backoff value
        if self.backoff_generator and len(self.completed_attempts) > 0:
            # find the attempt's backoff by sending attempt number to generator
            # generator will return the backoff time in seconds, so convert to ms
            backoff_ms = (
                self.backoff_generator.send(len(self.completed_attempts) - 1) * 1000
            )
        else:
            backoff_ms = 0

        self.active_attempt = ActiveAttemptMetric(backoff_before_attempt_ms=backoff_ms)

    def add_response_metadata(self, metadata: dict[str, bytes | str]) -> None:
        """
        Attach trailing metadata to the active attempt.

        If not called, default values for the metadata will be used.

        Assumes operation is in ACTIVE_ATTEMPT state.

        Args:
          - metadata: the metadata as extracted from the grpc call
        """
        if self.state != OperationState.ACTIVE_ATTEMPT:
            return self._handle_error(
                INVALID_STATE_ERROR.format("add_response_metadata", self.state)
            )
        if self.cluster_id is None or self.zone is None:
            # BIGTABLE_METADATA_KEY should give a binary-encoded ResponseParams proto
            blob = cast(bytes, metadata.get(BIGTABLE_METADATA_KEY))
            if blob:
                parse_result = self._parse_response_metadata_blob(blob)
                if parse_result is not None:
                    cluster, zone = parse_result
                    if cluster:
                        self.cluster_id = cluster
                    if zone:
                        self.zone = zone
                else:
                    self._handle_error(
                        f"Failed to decode {BIGTABLE_METADATA_KEY} metadata: {blob!r}"
                    )
        # SERVER_TIMING_METADATA_KEY should give a string with the server-latency headers
        timing_header = cast(str, metadata.get(SERVER_TIMING_METADATA_KEY))
        if timing_header:
            timing_data = SERVER_TIMING_REGEX.match(timing_header)
            if timing_data and self.active_attempt:
                self.active_attempt.gfe_latency_ms = float(timing_data.group(1))

    @staticmethod
    @lru_cache(maxsize=32)
    def _parse_response_metadata_blob(blob: bytes) -> Tuple[str, str] | None:
        """
        Parse the response metadata blob and return a tuple of cluster and zone.

        Function is cached to avoid parsing the same blob multiple times.

        Args:
          - blob: the metadata blob as extracted from the grpc call
        Returns:
          - a tuple of cluster_id and zone, or None if parsing failed
        """
        try:
            proto = ResponseParams.pb().FromString(blob)
            return proto.cluster_id, proto.zone_id
        except (DecodeError, TypeError):
            # failed to parse metadata
            return None

    def attempt_first_response(self) -> None:
        """
        Called to mark the timestamp of the first completed response for the
        active attempt.

        Assumes operation is in ACTIVE_ATTEMPT state.
        """
        if self.state != OperationState.ACTIVE_ATTEMPT or self.active_attempt is None:
            return self._handle_error(
                INVALID_STATE_ERROR.format("attempt_first_response", self.state)
            )
        if self.active_attempt.first_response_latency_ms is not None:
            return self._handle_error("Attempt already received first response")
        # convert duration to milliseconds
        self.active_attempt.first_response_latency_ms = (
            time.monotonic() - self.active_attempt.start_time.monotonic
        ) * 1000

    def end_attempt_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of an attempt for the operation.

        Typically, this is used to mark a retryable error. If a retry will not
        be attempted, `end_with_status` or `end_with_success` should be used
        to finalize the operation along with the attempt.

        Assumes operation is in ACTIVE_ATTEMPT state.

        Args:
          - status: The status of the attempt.
        """
        if self.state != OperationState.ACTIVE_ATTEMPT or self.active_attempt is None:
            return self._handle_error(
                INVALID_STATE_ERROR.format("end_attempt_with_status", self.state)
            )
        if isinstance(status, Exception):
            status = self._exc_to_status(status)
        duration_seconds = time.monotonic() - self.active_attempt.start_time.monotonic
        new_attempt = CompletedAttemptMetric(
            start_time=self.active_attempt.start_time.utc,
            first_response_latency_ms=self.active_attempt.first_response_latency_ms,
            duration_ms=duration_seconds * 1000,
            end_status=status,
            gfe_latency_ms=self.active_attempt.gfe_latency_ms,
            application_blocking_time_ms=self.active_attempt.application_blocking_time_ms,
            backoff_before_attempt_ms=self.active_attempt.backoff_before_attempt_ms,
            grpc_throttling_time_ms=self.active_attempt.grpc_throttling_time_ms,
        )
        self.completed_attempts.append(new_attempt)
        self.active_attempt = None
        for handler in self.handlers:
            handler.on_attempt_complete(new_attempt, self)

    def end_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of the operation. If there is an active attempt,
        end_attempt_with_status will be called with the same status.

        Assumes operation is not already in COMPLETED state.

        Causes on_operation_completed to be called for each registered handler.

        Args:
          - status: The status of the operation.
        """
        if self.state == OperationState.COMPLETED:
            return self._handle_error(
                INVALID_STATE_ERROR.format("end_with_status", self.state)
            )
        final_status = (
            self._exc_to_status(status) if isinstance(status, Exception) else status
        )
        if self.state == OperationState.ACTIVE_ATTEMPT:
            self.end_attempt_with_status(final_status)
        self.was_completed = True
        finalized = CompletedOperationMetric(
            op_type=self.op_type,
            start_time=self.start_time.utc,
            completed_attempts=self.completed_attempts,
            duration_ms=(time.monotonic() - self.start_time.monotonic) * 1000,
            final_status=final_status,
            cluster_id=self.cluster_id or DEFAULT_CLUSTER_ID,
            zone=self.zone or DEFAULT_ZONE,
            is_streaming=self.is_streaming,
            flow_throttling_time_ms=self.flow_throttling_time_ms,
        )
        for handler in self.handlers:
            handler.on_operation_complete(finalized)

    def end_with_success(self):
        """
        Called to mark the end of the operation with a successful status.

        Assumes operation is not already in COMPLETED state.

        Causes on_operation_completed to be called for each registered handler.
        """
        return self.end_with_status(StatusCode.OK)

    def build_wrapped_predicate(
        self, inner_predicate: Callable[[Exception], bool]
    ) -> Callable[[Exception], bool]:
        """
        Wrapps a predicate to include metrics tracking. Any call to the resulting predicate
        is assumed to be an rpc failure, and will either mark the end of the active attempt
        or the end of the operation.

        Args:
          - predicate: The predicate to wrap.
        """

        def wrapped_predicate(exc: Exception) -> bool:
            inner_result = inner_predicate(exc)
            if inner_result:
                self.end_attempt_with_status(exc)
            else:
                self.end_with_status(exc)
            return inner_result

        return wrapped_predicate

    @staticmethod
    def _exc_to_status(exc: Exception) -> StatusCode:
        """
        Extracts the grpc status code from an exception.

        Exception groups and wrappers will be parsed to find the underlying
        grpc Exception.

        If the exception is not a grpc exception, will return StatusCode.UNKNOWN.

        Args:
          - exc: The exception to extract the status code from.
        """
        if isinstance(exc, bt_exceptions._BigtableExceptionGroup):
            exc = exc.exceptions[-1]
        if hasattr(exc, "grpc_status_code") and exc.grpc_status_code is not None:
            return exc.grpc_status_code
        if (
            exc.__cause__
            and hasattr(exc.__cause__, "grpc_status_code")
            and exc.__cause__.grpc_status_code is not None
        ):
            return exc.__cause__.grpc_status_code
        return StatusCode.UNKNOWN

    @staticmethod
    def _handle_error(message: str) -> None:
        """
        log error metric system error messages

        Args:
          - message: The message to include in the exception or warning.
        """
        full_message = f"Error in Bigtable Metrics: {message}"
        LOGGER.warning(full_message)

    async def __aenter__(self):
        """
        Implements the async context manager protocol for wrapping unary calls

        Using the operation's context manager provides assurances that the operation
        is always closed when complete, with the proper status code automaticallty
        detected when an exception is raised.
        """
        return self._AsyncContextManager(self)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Implements the async context manager protocol for wrapping unary calls

        The operation is automatically ended on exit, with the status determined
        by the exception type and value.
        """
        if exc_val is None:
            self.end_with_success()
        else:
            self.end_with_status(exc_val)

    class _AsyncContextManager:
        """
        Inner class for async context manager protocol

        This class provides functions for wrapping unary gapic functions,
        and automatically tracking the metrics associated with the call
        """

        def __init__(self, operation: ActiveOperationMetric):
            self.operation = operation

        def add_response_metadata(self, metadata):
            """
            Pass through trailing metadata to the wrapped operation
            """
            return self.operation.add_response_metadata(metadata)

        def wrap_attempt_fn(
            self,
            fn: Callable[..., Any],
            *,
            extract_call_metadata: bool = True,
        ) -> Callable[..., Any]:
            """
            Wraps a function call, tracing metadata along the way

            Typically, the wrapped function will be a gapic rpc call

            Args:
              - fn: The function to wrap
              - extract_call_metadata: If True, the call will be treated as a
                  grpc function, and will automatically extract trailing_metadata
                  from the Call object on success.
            """

            async def wrapped_fn(*args, **kwargs):
                encountered_exc: Exception | None = None
                call = None
                self.operation.start_attempt()
                try:
                    call = fn(*args, **kwargs)
                    return await call
                except Exception as e:
                    encountered_exc = e
                    raise
                finally:
                    # capture trailing metadata
                    if extract_call_metadata and call is not None:
                        metadata = (
                            await call.trailing_metadata()
                            + await call.initial_metadata()
                        )
                        self.operation.add_response_metadata(metadata)
                    if encountered_exc is not None:
                        # end attempt. Let higher levels decide when to end operation
                        self.operation.end_attempt_with_status(encountered_exc)

            return wrapped_fn
