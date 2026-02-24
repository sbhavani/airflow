# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""User-defined metrics support for Airflow operators."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow.sdk.observability.metrics import AggregationFunction, MetricType

if TYPE_CHECKING:
    from datetime import datetime

log = logging.getLogger(__name__)


class UserDefinedMetrics:
    """
    Class for emitting user-defined custom metrics to the metadata database.

    This class allows operators to emit custom metrics during task execution.
    Metrics are stored in the Airflow metadata database and can be queried
    with automatic aggregation functions.

    Example usage in an operator::

        from airflow.sdk import BaseOperator, UserDefinedMetrics

        class MyOperator(BaseOperator):
            def execute(self, context):
                metrics = UserDefinedMetrics(context)

                # Emit a simple metric
                metrics.emit(
                    metric_name="records_processed",
                    value=1000,
                    aggregation=AggregationFunction.SUM,
                )

                # Emit a metric with labels and tags
                metrics.emit(
                    metric_name="processing_time",
                    value=45.5,
                    metric_label="step_1",
                    aggregation=AggregationFunction.AVG,
                    unit="seconds",
                    tags={"dataset": "sales", "region": "us-east"},
                )

                # Emit multiple metrics at once
                metrics.emit_batch([
                    {"metric_name": "records_read", "value": 500, "aggregation": AggregationFunction.SUM},
                    {"metric_name": "records_written", "value": 450, "aggregation": AggregationFunction.SUM},
                    {"metric_name": "error_count", "value": 5, "aggregation": AggregationFunction.MAX},
                ])
    """

    def __init__(self, context: dict[str, Any]):
        """
        Initialize UserDefinedMetrics with task context.

        :param context: The task context dictionary (TiMixin context)
        """
        self._context = context
        self._client = None

    @property
    def _api_client(self):
        """Lazy-load the API client to avoid circular imports."""
        if self._client is None:
            from airflow.sdk.execution_time.comms import get_dag_context_var

            dag_context = get_dag_context_var()
            self._client = dag_context.get("client")
        return self._client

    def _get_task_info(self) -> tuple[str, str, str, int]:
        """Extract task information from context."""
        dag_id = self._context.get("dag", {}).get("dag_id", "")
        task_id = self._context.get("task_instance_key_str", "")
        run_id = self._context.get("run_id", "")
        map_index = self._context.get("map_index", -1)

        # Parse task_id from task_instance_key_str if needed
        if not task_id and "task_instance" in self._context:
            ti = self._context["task_instance"]
            task_id = ti.task_id
            dag_id = ti.dag_id
            run_id = ti.run_id

        return dag_id, task_id, run_id, map_index

    def emit(
        self,
        metric_name: str,
        value: float,
        metric_type: MetricType | None = None,
        aggregation: AggregationFunction | None = None,
        metric_label: str | None = None,
        unit: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> int | None:
        """
        Emit a single user-defined metric.

        :param metric_name: Name of the metric (required)
        :param value: Numeric value of the metric (required)
        :param metric_type: Type of metric for automatic aggregation (COUNT, GAUGE, TIMING).
            If provided, aggregation will be automatically selected based on metric type.
        :param aggregation: Aggregation function to use (default: SUM if not using metric_type).
            Ignored if metric_type is provided.
        :param metric_label: Optional label to differentiate metric instances
        :param unit: Optional unit of measurement (e.g., "seconds", "bytes")
        :param tags: Optional dictionary of tags for filtering
        :return: The ID of the created metric, or None if emission failed
        """
        if not metric_name:
            raise ValueError("metric_name is required")

        # Auto-select aggregation based on metric_type
        if metric_type is not None:
            aggregation = metric_type.default_aggregation
        elif aggregation is None:
            aggregation = AggregationFunction.SUM

        dag_id, task_id, run_id, map_index = self._get_task_info()

        payload = {
            "metric_name": metric_name,
            "metric_label": metric_label,
            "value": value,
            "aggregation": aggregation.value if isinstance(aggregation, AggregationFunction) else aggregation,
            "unit": unit,
            "tags": tags,
        }

        try:
            response = self._api_client.post(
                f"user-defined-metrics/{dag_id}/{run_id}/{task_id}",
                params={"map_index": map_index},
                json=payload,
            )
            response.raise_for_status()
            result = response.json()
            log.debug(
                "Emitted metric: name=%s, value=%s, aggregation=%s",
                metric_name,
                value,
                aggregation,
            )
            return result.get("id")
        except Exception as e:
            log.warning(
                "Failed to emit metric '%s': %s. Metrics will not be stored in the database.",
                metric_name,
                str(e),
            )
            return None

    def emit_batch(
        self,
        metrics: list[dict[str, Any]],
    ) -> list[int]:
        """
        Emit multiple user-defined metrics at once.

        This is more efficient than calling emit() multiple times.

        :param metrics: List of metric dictionaries. Each dictionary should contain:
            - metric_name: str (required)
            - value: float (required)
            - aggregation: AggregationFunction (optional, default: SUM)
            - metric_label: str | None (optional)
            - unit: str | None (optional)
            - tags: dict[str, str] | None (optional)
        :return: List of IDs of created metrics
        """
        if not metrics:
            return []

        dag_id, task_id, run_id, map_index = self._get_task_info()

        payload = {
            "metrics": [
                {
                    "metric_name": m["metric_name"],
                    "metric_label": m.get("metric_label"),
                    "value": m["value"],
                    "aggregation": (
                        m["aggregation"].value
                        if isinstance(m.get("aggregation"), AggregationFunction)
                        else m.get("aggregation", AggregationFunction.SUM.value)
                    ),
                    "unit": m.get("unit"),
                    "tags": m.get("tags"),
                }
                for m in metrics
            ]
        }

        try:
            response = self._api_client.post(
                f"user-defined-metrics/{dag_id}/{run_id}/{task_id}/batch",
                params={"map_index": map_index},
                json=payload,
            )
            response.raise_for_status()
            result = response.json()
            log.debug(
                "Emitted %d metrics in batch",
                len(result.get("ids", [])),
            )
            return result.get("ids", [])
        except Exception as e:
            log.warning(
                "Failed to emit metrics batch: %s. Metrics will not be stored in the database.",
                str(e),
            )
            return []


# For backwards compatibility
UserDefinedMetric = UserDefinedMetrics
