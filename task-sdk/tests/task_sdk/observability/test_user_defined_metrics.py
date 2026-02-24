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
"""Tests for UserDefinedMetrics class."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.sdk.observability.user_defined_metrics import AggregationFunction, UserDefinedMetrics


class TestAggregationFunction:
    """Test AggregationFunction enum."""

    def test_sum(self):
        assert AggregationFunction.SUM == AggregationFunction.SUM
        assert AggregationFunction.SUM.value == "sum"

    def test_avg(self):
        assert AggregationFunction.AVG.value == "avg"

    def test_min(self):
        assert AggregationFunction.MIN.value == "min"

    def test_max(self):
        assert AggregationFunction.MAX.value == "max"

    def test_count(self):
        assert AggregationFunction.COUNT.value == "count"

    def test_last(self):
        assert AggregationFunction.LAST.value == "last"


class TestUserDefinedMetrics:
    """Test UserDefinedMetrics class."""

    def test_init(self):
        """Test initialization with context."""
        context = {"dag": {"dag_id": "test_dag"}, "run_id": "test_run"}
        metrics = UserDefinedMetrics(context)
        assert metrics._context == context

    @patch("airflow.sdk.observability.user_defined_metrics.get_dag_context_var")
    def test_emit_success(self, mock_get_context):
        """Test successful metric emission."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 123}
        mock_client.post.return_value = mock_response

        mock_context = {"client": mock_client}
        mock_get_context.return_value = mock_context

        context = {
            "dag": {"dag_id": "test_dag"},
            "run_id": "test_run",
            "task_instance_key_str": "test_task",
            "map_index": -1,
        }
        metrics = UserDefinedMetrics(context)

        result = metrics.emit(
            metric_name="test_metric",
            value=42.0,
            aggregation=AggregationFunction.SUM,
        )

        assert result == 123
        mock_client.post.assert_called_once()

    @patch("airflow.sdk.observability.user_defined_metrics.get_dag_context_var")
    def test_emit_with_all_params(self, mock_get_context):
        """Test metric emission with all parameters."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 456}
        mock_client.post.return_value = mock_response

        mock_context = {"client": mock_client}
        mock_get_context.return_value = mock_context

        context = {
            "dag": {"dag_id": "my_dag"},
            "run_id": "my_run",
            "task_instance_key_str": "my_task",
            "map_index": 0,
        }
        metrics = UserDefinedMetrics(context)

        result = metrics.emit(
            metric_name="records_processed",
            value=1000,
            aggregation=AggregationFunction.AVG,
            metric_label="batch_1",
            unit="records",
            tags={"environment": "prod"},
        )

        assert result == 456

        # Verify the call was made with correct parameters
        call_args = mock_client.post.call_args
        assert "user-defined-metrics/my_dag/my_run/my_task" in call_args[0][0]
        assert call_args[1]["params"]["map_index"] == 0

    @patch("airflow.sdk.observability.user_defined_metrics.get_dag_context_var")
    def test_emit_batch_success(self, mock_get_context):
        """Test successful batch metric emission."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"count": 3, "ids": [1, 2, 3]}
        mock_client.post.return_value = mock_response

        mock_context = {"client": mock_client}
        mock_get_context.return_value = mock_context

        context = {
            "dag": {"dag_id": "test_dag"},
            "run_id": "test_run",
            "task_instance_key_str": "test_task",
            "map_index": -1,
        }
        metrics = UserDefinedMetrics(context)

        result = metrics.emit_batch([
            {"metric_name": "metric1", "value": 10, "aggregation": AggregationFunction.SUM},
            {"metric_name": "metric2", "value": 20, "aggregation": AggregationFunction.MAX},
            {"metric_name": "metric3", "value": 30, "aggregation": AggregationFunction.COUNT},
        ])

        assert result == [1, 2, 3]
        mock_client.post.assert_called_once()

    @patch("airflow.sdk.observability.user_defined_metrics.get_dag_context_var")
    def test_emit_failure(self, mock_get_context):
        """Test metric emission failure handling."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_client.post.return_value = mock_response

        mock_context = {"client": mock_client}
        mock_get_context.return_value = mock_context

        context = {
            "dag": {"dag_id": "test_dag"},
            "run_id": "test_run",
            "task_instance_key_str": "test_task",
            "map_index": -1,
        }
        metrics = UserDefinedMetrics(context)

        # Should not raise, just log warning and return None
        result = metrics.emit(metric_name="test_metric", value=42.0)
        assert result is None

    def test_emit_empty_metric_name_raises(self):
        """Test that empty metric name raises ValueError."""
        context = {"dag": {"dag_id": "test_dag"}, "run_id": "test_run"}
        metrics = UserDefinedMetrics(context)

        with pytest.raises(ValueError, match="metric_name is required"):
            metrics.emit(metric_name="", value=42.0)

    @patch("airflow.sdk.observability.user_defined_metrics.get_dag_context_var")
    def test_emit_batch_empty_list(self, mock_get_context):
        """Test batch emission with empty list returns empty list."""
        mock_client = MagicMock()
        mock_context = {"client": mock_client}
        mock_get_context.return_value = mock_context

        context = {"dag": {"dag_id": "test_dag"}, "run_id": "test_run", "task_instance_key_str": "test_task"}
        metrics = UserDefinedMetrics(context)

        result = metrics.emit_batch([])
        assert result == []
        mock_client.post.assert_not_called()
