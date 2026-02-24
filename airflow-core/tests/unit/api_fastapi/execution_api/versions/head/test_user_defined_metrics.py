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
"""Tests for user-defined metrics API."""
from __future__ import annotations

import pytest

from airflow.models.user_defined_metric import AggregationFunction, UserDefinedMetric
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def reset_db():
    """Reset user_defined_metric table."""
    with create_session() as session:
        session.query(UserDefinedMetric).delete()
        session.commit()


class TestUserDefinedMetricModel:
    """Test UserDefinedMetric model."""

    def test_create_metric(self, session):
        """Test creating a basic metric."""
        from datetime import datetime, timezone

        metric = UserDefinedMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            metric_name="test_metric",
            value=42.0,
            aggregation=AggregationFunction.SUM.value,
            timestamp=datetime.now(timezone.utc),
        )
        session.add(metric)
        session.commit()

        # Refresh to get the ID
        session.refresh(metric)

        assert metric.id is not None
        assert metric.dag_id == "test_dag"
        assert metric.task_id == "test_task"
        assert metric.metric_name == "test_metric"
        assert metric.value == 42.0
        assert metric.aggregation == "sum"

    def test_metric_with_label(self, session):
        """Test creating a metric with a label."""
        from datetime import datetime, timezone

        metric = UserDefinedMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            metric_name="test_metric",
            metric_label="label1",
            value=100.0,
            aggregation=AggregationFunction.AVG.value,
            timestamp=datetime.now(timezone.utc),
        )
        session.add(metric)
        session.commit()

        assert metric.metric_label == "label1"

    def test_metric_with_tags(self, session):
        """Test creating a metric with tags."""
        import json
        from datetime import datetime, timezone

        tags = {"environment": "prod", "region": "us-east"}
        metric = UserDefinedMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            metric_name="test_metric",
            value=50.0,
            aggregation=AggregationFunction.MAX.value,
            timestamp=datetime.now(timezone.utc),
            tags=json.dumps(tags),
        )
        session.add(metric)
        session.commit()

        assert json.loads(metric.tags) == tags


class TestAggregationFunction:
    """Test AggregationFunction enum."""

    def test_all_aggregation_functions(self):
        """Test all aggregation function values."""
        assert AggregationFunction.SUM.value == "sum"
        assert AggregationFunction.AVG.value == "avg"
        assert AggregationFunction.MIN.value == "min"
        assert AggregationFunction.MAX.value == "max"
        assert AggregationFunction.COUNT.value == "count"
        assert AggregationFunction.LAST.value == "last"
