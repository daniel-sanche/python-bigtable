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

from uuid import uuid4

from google.api.monitored_resource_pb2 import MonitoredResource  # type: ignore
from google.cloud.bigtable import __version__ as bigtable_version
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import OperationType
from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedAttemptMetric


class OpenTelemetryMetricsHandler(MetricsHandler):
    """
    Maintains a set of OpenTelemetry metrics for the Bigtable client library,
    and updates them with each completed operation and attempt.

    The OpenTelemetry metrics that are tracked are as follows:
      - operation_latencies: latency of each client method call, over all of it's attempts.
      - first_response_latencies: latency of receiving the first row in a ReadRows operation.
      - attempt_latencies: latency of each client attempt RPC.
      - retry_count: Number of additional RPCs sent after the initial attempt.
      - server_latencies: latency recorded on the server side for each attempt.
      - connectivity_error_count: number of attempts that failed to reach Google's network.
      - application_latencies: the time spent waiting for the application to process the next response.
      - throttling_latencies: latency introduced by waiting when there are too many outstanding requests in a bulk operation.
    """

    def __init__(self, *, project_id:str, instance_id:str, table_id:str, app_profile_id:str | None, client_uid:str | None = None, **kwargs):
        super().__init__()
        from opentelemetry import metrics

        meter = metrics.get_meter(__name__)
        self.op_latency = meter.create_histogram(
            name="operation_latencies",
            description="A distribution of the latency of each client method call, across all of it's RPC attempts",
            unit="ms",
        )
        self.first_response_latency = meter.create_histogram(
            name="first_response_latencies",
            description="A distribution of the latency of receiving the first row in a ReadRows operation.",
            unit="ms",
        )
        self.attempt_latency = meter.create_histogram(
            name="attempt_latencies",
            description="A distribution of the latency of each client RPC, tagged by operation name and the attempt status. Under normal circumstances, this will be identical to op_latency. However, when the client receives transient errors, op_latency will be the sum of all attempt_latencies and the exponential delays.",
            unit="ms",
        )
        self.retry_count = meter.create_counter(
            name="retry_count",
            description="A count of additional RPCs sent after the initial attempt. Under normal circumstances, this will be 1.",
        )
        self.server_latency = meter.create_histogram(
            name="server_latencies",
            description="A distribution of the latency measured between the time when Google's frontend receives an RPC and sending back the first byte of the response.",
        )
        self.connectivity_error_count = meter.create_counter(
            name="connectivity_error_count",
            description="A count of the number of attempts that failed to reach Google's network.",
        )
        self.server_latency = meter.create_histogram(
            name="server_latencies",
            description="A distribution of the latency measured between the time when Google's frontend receives an RPC and sending back the first byte of the response.",
        )
        self.shared_labels = {
            "client_name": f"python-bigtable/{bigtable_version}",
            "client_uid": client_uid or str(uuid4()),
        }
        if app_profile_id:
            self.shared_labels["bigtable_app_profile_id"] = app_profile_id
        self.monitored_resource_labels = {
            "project": project_id,
            "instance": instance_id,
            "table": table_id,
        }

    def on_operation_complete(self, op: CompletedOperationMetric) -> None:
        """
        Update the metrics associated with a completed operation:
          - operation_latencies
          - retry_count
        """
        labels = {"method": op.op_type.value, "status": op.final_status, "streaming": op.is_streaming, **self.shared_labels}
        monitored_resource = MonitoredResource(type="bigtable_client_raw", labels={"zone": op.zone, **self.monitored_resource_labels})
        if op.cluster_id is not None:
            monitored_resource.labels["cluster"] = op.cluster_id

        self.op_latency.record(op.duration, labels)
        self.retry_count.add(len(op.completed_attempts)-1, labels)

    def on_attempt_complete(self, attempt: CompletedAttemptMetric, op: ActiveOperationMetric) -> None:
        """
        Update the metrics associated with a completed attempt:
          - attempt_latencies
          - first_response_latencies
          - server_latencies
          - connectivity_error_count
        """
        labels = {"method": op.op_type.value, "status": attempt.end_status.value, "streaming":op.is_streaming, **self.shared_labels}
        monitored_resource = MonitoredResource(type="bigtable_client_raw", labels={"zone": op.zone, **self.monitored_resource_labels})
        if op.cluster_id is not None:
            monitored_resource.labels["cluster"] = op.cluster_id

        self.attempt_latency.record(attempt.duration, labels)
        if op.op_type == OperationType.READ_ROWS and attempt.first_response_latency is not None:
            self.first_response_latency.record(attempt.first_response_latency, labels)
        if attempt.gfe_latency is not None:
            self.server_latency.record(attempt.gfe_latency, labels)
        else:
            # gfe headers not attached. Record a connectivity error.
            # TODO: this should not be recorded as an error when direct path is enabled
            self.connectivity_error_count.add(1, labels)
