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

from typing import Tuple

from uuid import uuid4
from uuid import UUID
import time
from dataclasses import dataclass


class BigtableClientSideMetrics():

    def __init__(self, project_id:str, instance_id:str, app_profile_id:str | None):
        from opentelemetry import metrics
        meter = metrics.get_meter(__name__)
        self.op_latency = meter.create_histogram(
            name="op_latency",
            description="A distribution of latency of each client method call, across all of it's RPC attempts. Tagged by operation name and final response status.",
            unit="ms",
            value_type=float,
        )
        self.completed_ops = meter.create_counter(
            name="completed_ops",
            description="The total count of method invocations. Tagged by operation name and final response status",
            unit="1",
            value_type=int,
        )
        self.read_rows_first_row_latency = meter.create_histogram(
            name="read_rows_first_row_latency",
            description="A distribution of the latency of receiving the first row in a ReadRows operation.",
            unit="ms",
            value_type=float,
        )
        self.attempt_latency = meter.create_histogram(
            name="attempt_latency",
            description="A distribution of latency of each client RPC, tagged by operation name and the attempt status. Under normal circumstances, this will be identical to op_latency. However, when the client receives transient errors, op_latency will be the sum of all attempt_latencies and the exponential delays.",
            unit="ms",
            value_type=float,
        )
        self.attempts_per_op = meter.create_histogram(
            name="attempts_per_op",
            description="A distribution of attempts that each operation required, tagged by operation name and final operation status. Under normal circumstances, this will be 1.",
            value_type=int,
        )
        self.shared_labels = {"bigtable_project_id": project_id, "bigtable_instance_id": instance_id}
        if app_profile_id:
            self.shared_labels["bigtable_app_profile_id"] = app_profile_id
        self._active_ops: dict[UUID, Tuple[float, float]] = {}

    def record_operation_start(self, op_type:str) -> UUID:
        start_time = time.monotonic()
        op_id = uuid4()
        self._active_ops[op_id] = _MetricOperation(op_type, start_time, start_time, 0)
        return op_id

    def record_attempt_start(self, op_id:UUID):
        self._active_ops[op_id].attempt_start_time = time.monotonic()

    def record_attempt_complete(self, op_id:UUID, status:int):
        op = self._active_ops[op_id]
        current_time = time.monotonic()
        attempt_latency = current_time - op.attempt_start_time
        op.attempt_start_time = current_time
        op.num_retries += 1
        self.attempt_latency.record(attempt_latency, {"op_name": op.name, "status": status})

    def record_operation_complete(self, op_id:UUID, status:int):
        op = self._active_ops[op_id]
        del self._active_ops[op_id]
        labels = {"op_name": op.name, "status": status, **self.shared_labels}
        op_latency = time.monotonic() - op.start_time
        self.completed_ops.add(1, labels)
        self.attempts_per_op.record(op.num_retries + 1, labels)
        self.op_latency.record(op_latency, labels)

    def record_read_rows_first_row_latency(self, latency):
        self.read_rows_first_row_latency.record(latency, self.shared_labels)


@dataclass
class _MetricOperation:
    name: str
    start_time: float
    attempt_start_time: float
    num_retries: int
