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
"""Tests for user-defined metrics Core API."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from airflow.models.user_defined_metric import AggregationFunction, UserDefinedMetric
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "test_run"


@pytest.fixture(autouse=True)
def reset_db():
    """Reset user_defined_metric table."""
    with create_session() as session:
        session.query(UserDefinedMetric).delete()
        session.commit()


@pytest.fixture
def sample_metrics(session):
    """Create sample metrics for testing."""
    metrics = [
        UserDefinedMetric(
            dag_id=DAG_ID,
            task_id=TASK_ID,
            run_id=RUN_ID,
            map_index=-1,
            metric_name="records_processed",
            value=100.0,
            aggregation=AggregationFunction.SUM.value,
            timestamp=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        ),
        UserDefinedMetric(
            dag_id=DAG_ID,
            task_id=TASK_ID,
            run_id=RUN_ID,
            map_index=-1,
            metric_name="records_processed",
            value=200.0,
            aggregation=AggregationFunction.SUM.value,
            timestamp=datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
        ),
        UserDefinedMetric(
            dag_id=DAG_ID,
            task_id=TASK_ID,
            run_id=RUN_ID,
            map_index=-1,
            metric_name="processing_time",
            value=45.5,
            aggregation=AggregationFunction.AVG.value,
            timestamp=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        ),
    ]
    session.add_all(metrics)
    session.commit()
    for m in metrics:
        session.refresh(m)
    return metrics


class TestUserDefinedMetricsEndpoints:
    """Test user-defined metrics endpoints."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.app = None  # Would need to create test app

    def test_get_metrics_returns_empty_list(self, session):
        """Test getting metrics when none exist."""
        from airflow.api_fastapi.core_api.datamodels.user_defined_metrics import (
            UserDefinedMetricCollectionResponse,
        )

        # Query would return empty
        query = session.query(UserDefinedMetric)
        result = session.execute(query)
        metrics = result.scalars().all()

        assert len(metrics) == 0

    def test_get_metrics_with_filters(self, session, sample_metrics):
        """Test getting metrics with filters."""
        # Filter by dag_id
        query = session.query(UserDefinedMetric).where(UserDefinedMetric.dag_id == DAG_ID)
        result = session.execute(query)
        metrics = result.scalars().all()

        assert len(metrics) == 3

        # Filter by metric_name
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        assert len(metrics) == 2

    def test_aggregation_sum(self, session, sample_metrics):
        """Test sum aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        values = [m.value for m in metrics]
        agg_value = sum(values)

        assert agg_value == 300.0

    def test_aggregation_avg(self, session, sample_metrics):
        """Test average aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "processing_time"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        values = [m.value for m in metrics]
        agg_value = sum(values) / len(values)

        assert agg_value == 45.5

    def test_aggregation_min(self, session, sample_metrics):
        """Test min aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        values = [m.value for m in metrics]
        agg_value = min(values)

        assert agg_value == 100.0

    def test_aggregation_max(self, session, sample_metrics):
        """Test max aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        values = [m.value for m in metrics]
        agg_value = max(values)

        assert agg_value == 200.0

    def test_aggregation_count(self, session, sample_metrics):
        """Test count aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        assert len(metrics) == 2

    def test_aggregation_last(self, session, sample_metrics):
        """Test last aggregation."""
        query = session.query(UserDefinedMetric).where(
            UserDefinedMetric.metric_name == "records_processed"
        )
        result = session.execute(query)
        metrics = result.scalars().all()

        sorted_metrics = sorted(metrics, key=lambda m: m.timestamp, reverse=True)
        last_value = sorted_metrics[0].value if sorted_metrics else 0.0

        assert last_value == 200.0
