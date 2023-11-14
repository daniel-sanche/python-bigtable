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

import time
import os

from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryMetricsHandler
from google.cloud.bigtable.data._metrics.handlers.stdout import StdoutMetricsHandler
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import OperationType


PRINT_METRICS = os.getenv("BIGTABLE_PRINT_METRICS", False)


class BigtableClientSideMetricsController():
    """
    BigtableClientSideMetricsController is responsible for managing the
    lifecycle of the metrics system. The Bigtable client library will
    use this class to create new operations. Each operation will be
    registered with the handlers associated with this controller.
    """

    def __init__(self, handlers:list[MetricsHandler]=None, **kwargs):
        """
        Initializes the metrics controller.

        Args:
          - handlers: A list of MetricsHandler objects to subscribe to metrics events.
          - **kwargs: Optional arguments to pass to the metrics handlers.
        """
        self.handlers: list[MetricsHandler] = handlers or []
        if handlers is None:
            # handlers not given. Use default handlers.
            if PRINT_METRICS:
                self.handlers.append(StdoutMetricsHandler(**kwargs))
            try:
                ot_handler = OpenTelemetryMetricsHandler(**kwargs)
                self.handlers.append(ot_handler)
            except ImportError:
                pass

    def add_handler(self, handler:MetricsHandler) -> None:
        """
        Add a new handler to the list of handlers.

        Args:
          - handler: A MetricsHandler object to add to the list of subscribed handlers.
        """
        self.handlers.append(handler)

    def create_operation(self, op_type:OperationType, is_streaming:bool = False) -> ActiveOperationMetric:
        """
        Creates a new operation and registers it with the subscribed handlers.

        Args:
          - op_type: The type of operation to create.
          - is_streaming: Whether the operation is a streaming operation. Should only be
                True for ReadRows operations.
        """
        start_time = time.monotonic()
        new_op = ActiveOperationMetric(
            op_type=op_type,
            start_time=start_time,
            _handlers=self.handlers,
            is_streaming=is_streaming,
        )
        return new_op
