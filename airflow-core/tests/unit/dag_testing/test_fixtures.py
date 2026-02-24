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
"""Unit tests for DAG testing fixtures."""

from __future__ import annotations

import pytest

from airflow.dag_testing.fixtures import (
    dag_with_tasks,
    mock_operator_fixture,
    task_instance_factory,
    test_context,
    test_dag,
)
from airflow.models import DAG


class TestTestDagFixture:
    """Tests for test_dag fixture."""

    def test_provides_dag(self, test_dag):
        """Test that fixture provides a DAG."""
        assert isinstance(test_dag, DAG)
        assert test_dag.dag_id == "test_dag"

    def test_dag_has_start_date(self, test_dag):
        """Test that DAG has start date."""
        assert test_dag.start_date is not None


class TestMockOperatorFixture:
    """Tests for mock_operator_fixture."""

    def test_provides_mock_operator(self, mock_operator_fixture):
        """Test that fixture provides a MockOperator."""
        from airflow.dag_testing.mock_operator import MockOperator

        assert isinstance(mock_operator_fixture, MockOperator)
        assert mock_operator_fixture.task_id == "test_task"

    def test_has_return_value(self, mock_operator_fixture):
        """Test that mock has return value configured."""
        assert mock_operator_fixture.return_value == {"status": "success"}


class TestTestContextFixture:
    """Tests for test_context fixture."""

    def test_provides_context(self, test_context):
        """Test that fixture provides context dict."""
        assert isinstance(test_context, dict)
        assert "execution_date" in test_context
        assert "ds" in test_context
        assert "ds_nodash" in test_context
        assert "ts" in test_context
        assert "ts_nodash" in test_context

    def test_context_has_dates(self, test_context):
        """Test that context has date values."""
        assert test_context["ds"] == test_context["execution_date"].strftime("%Y-%m-%d")
        assert test_context["ds_nodash"] == test_context["execution_date"].strftime("%Y%m%d")


class TestTaskInstanceFactory:
    """Tests for task_instance_factory fixture."""

    def test_creates_task_instance(self, task_instance_factory):
        """Test that factory creates task instances."""
        from airflow.operators.empty import EmptyOperator
        from airflow.utils.state import State

        dag = DAG(dag_id="factory_test", start_date=None)
        with dag:
            task = EmptyOperator(task_id="factory_task")

        ti = task_instance_factory(task, State.QUEUED)
        assert ti.task_id == "factory_task"
        assert ti.state == State.QUEUED


class TestDagWithTasks:
    """Tests for dag_with_tasks fixture."""

    def test_provides_dag_with_tasks(self, dag_with_tasks):
        """Test that fixture provides a DAG with tasks."""
        assert dag_with_tasks.dag_id == "test_dag"
        assert len(dag_with_tasks.tasks) == 3

    def test_dag_has_correct_dependencies(self, dag_with_tasks):
        """Test that tasks have correct dependencies."""
        task_ids = [t.task_id for t in dag_with_tasks.tasks]
        assert "task_1" in task_ids
        assert "task_2" in task_ids
        assert "task_3" in task_ids
