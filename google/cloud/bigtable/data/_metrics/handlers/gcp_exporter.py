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
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics import view
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    MetricExporter,
    MetricExportResult,
    MetricsData,
    NumberDataPoint,
    PeriodicExportingMetricReader,
)
from google.protobuf.timestamp_pb2 import Timestamp
from google.api.distribution_pb2 import Distribution
from google.api.metric_pb2 import Metric as GMetric
from google.api.monitored_resource_pb2 import MonitoredResource
from google.api.metric_pb2 import MetricDescriptor
from google.cloud.monitoring_v3 import (
    CreateTimeSeriesRequest,
    MetricServiceClient,
    Point,
    TimeInterval,
    TimeSeries,
    TypedValue,
)

from google.cloud.bigtable.data._metrics.handlers.opentelemetry import (
    OpenTelemetryMetricsHandler,
)
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import (
    _OpenTelemetryInstrumentation,
)


MAX_BATCH_WRITE = 200


class TestExporter(MetricExporter):
    def __init__(self):
        super().__init__()
        self.client = MetricServiceClient()
        self.prefix = "bigtable.googleapis.com/internal/client"

    def export(
        self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs
    ) -> MetricExportResult:
        # cumulative used for all metrics
        metric_kind = MetricDescriptor.MetricKind.CUMULATIVE
        all_series: dict[str, list[TimeSeries]] = {}
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    for data_point in [
                        pt for pt in metric.data.data_points if pt.attributes
                    ]:
                        if data_point.attributes:
                            project_id = data_point.attributes["resource_project"]
                            if not isinstance(project_id, str):
                                # we expect string for project_id field
                                continue
                            monitored_resource = MonitoredResource(
                                type="bigtable_client_raw",
                                labels={
                                    "project_id": project_id,
                                    "instance": data_point.attributes[
                                        "resource_instance"
                                    ],
                                    "cluster": data_point.attributes[
                                        "resource_cluster"
                                    ],
                                    "table": data_point.attributes["resource_table"],
                                    "zone": data_point.attributes["resource_zone"],
                                },
                            )
                            point = self._to_point(data_point)
                            series = TimeSeries(
                                resource=monitored_resource,
                                metric_kind=metric_kind,
                                points=[point],
                                metric=GMetric(
                                    type=f"{self.prefix}/{metric.name}",
                                    labels={
                                        k: v
                                        for k, v in data_point.attributes.items()
                                        if not k.startswith("resource_")
                                    },
                                ),
                                unit=metric.unit,
                            )
                            project_series: list[TimeSeries] = all_series.get(
                                project_id, []
                            )
                            project_series.append(series)
                            all_series[project_id] = project_series
        try:
            for project_id, project_series in all_series.items():
                self._batch_write(project_id, project_series)
            return MetricExportResult.SUCCESS
        except Exception:
            return MetricExportResult.FAILURE

    def _batch_write(self, project_id: str, series: list[TimeSeries]) -> None:
        """Cloud Monitoring allows writing up to 200 time series at once

        Adapted from CloudMonitoringMetricsExporter
        https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/3668dfe7ce3b80dd01f42af72428de957b58b316/opentelemetry-exporter-gcp-monitoring/src/opentelemetry/exporter/cloud_monitoring/__init__.py#L82
        """
        write_ind = 0
        while write_ind < len(series):
            self.client.create_service_time_series(
                CreateTimeSeriesRequest(
                    name=f"projects/{project_id}",
                    time_series=series[write_ind : write_ind + MAX_BATCH_WRITE],
                ),
            )
            write_ind += MAX_BATCH_WRITE

    @staticmethod
    def _to_point(data_point: NumberDataPoint | HistogramDataPoint) -> Point:
        """
        Adapted from CloudMonitoringMetricsExporter
        https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/3668dfe7ce3b80dd01f42af72428de957b58b316/opentelemetry-exporter-gcp-monitoring/src/opentelemetry/exporter/cloud_monitoring/__init__.py#L82
        """
        if isinstance(data_point, HistogramDataPoint):
            mean = data_point.sum / data_point.count if data_point.count else 0.0
            point_value = TypedValue(
                distribution_value=Distribution(
                    count=data_point.count,
                    mean=mean,
                    bucket_counts=data_point.bucket_counts,
                    bucket_options=Distribution.BucketOptions(
                        explicit_buckets=Distribution.BucketOptions.Explicit(
                            bounds=data_point.explicit_bounds,
                        )
                    ),
                )
            )
        else:
            if isinstance(data_point.value, int):
                point_value = TypedValue(int64_value=data_point.value)
            else:
                point_value = TypedValue(double_value=data_point.value)
        start_time = Timestamp()
        start_time.FromNanoseconds(data_point.start_time_unix_nano)
        end_time = Timestamp()
        end_time.FromNanoseconds(data_point.time_unix_nano)
        interval = TimeInterval(start_time=start_time, end_time=end_time)
        return Point(interval=interval, value=point_value)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs):
        pass

    def force_flush(self, timeout_millis: float = 10_000):
        return True


def _create_private_meter_provider() -> MeterProvider:
    """
    Creates a private MeterProvider to store instruments and views, and send them
    periodically through a custom GCP metrics exporter
    """
    millis_aggregation = view.ExplicitBucketHistogramAggregation(
        [
            0.0,
            0.01,
            0.05,
            0.1,
            0.3,
            0.6,
            0.8,
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            8.0,
            10.0,
            13.0,
            16.0,
            20.0,
            25.0,
            30.0,
            40.0,
            50.0,
            65.0,
            80.0,
            100.0,
            130.0,
            160.0,
            200.0,
            250.0,
            300.0,
            400.0,
            500.0,
            650.0,
            800.0,
            1000.0,
            2000.0,
            5000.0,
            10000.0,
            20000.0,
            50000.0,
            100000.0,
        ]
    )
    count_aggregation = view.SumAggregation()
    view_list = [
        view.View(
            instrument_name="operation_latencies",
            aggregation=millis_aggregation,
            name="operation_latencies",
        ),
        view.View(
            instrument_name="first_response_latencies",
            aggregation=millis_aggregation,
            name="first_response_latencies",
        ),
        view.View(
            instrument_name="attempt_latencies",
            aggregation=millis_aggregation,
            name="attempt_latencies",
        ),
        view.View(
            instrument_name="retry_counts",
            aggregation=count_aggregation,
            name="retry_counts",
        ),
        view.View(
            instrument_name="server_latencies",
            aggregation=millis_aggregation,
            name="server_latencies",
        ),
        view.View(
            instrument_name="connectivity_error_count",
            aggregation=count_aggregation,
            name="connectivity_error_count",
        ),
        view.View(
            instrument_name="application_latencies",
            aggregation=millis_aggregation,
            name="application_latencies",
        ),
        view.View(
            instrument_name="throttling_latencies",
            aggregation=millis_aggregation,
            name="throttling_latencies",
        ),
    ]
    # writes metrics into GCP timeseries objects
    exporter = TestExporter()
    # periodically executes exporter
    gcp_reader = PeriodicExportingMetricReader(exporter, export_interval_millis=60_000)
    # set up root meter provider
    meter_provider = MeterProvider(metric_readers=[gcp_reader], views=view_list)
    return meter_provider


class GCPOpenTelemetryExporterHandler(OpenTelemetryMetricsHandler):
    # Otel instrumentation must use custom meter provider
    otel = _OpenTelemetryInstrumentation(_create_private_meter_provider())
