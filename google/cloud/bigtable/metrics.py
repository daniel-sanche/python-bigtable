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


class BigtableClientSideMetrics():

    def __init__(self):
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
