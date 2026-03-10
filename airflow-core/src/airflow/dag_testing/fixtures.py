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
"""Pytest Fixtures for DAG Testing.

This module provides reusable pytest fixtures for DAG testing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from airflow.models import DAG
from airflow.models.operator import Operator
from airflow.sdk import BaseOperator

from airflow.dag_testing.mock_operator import MockOperator
from airflow.dag_testing.task_simulator import TaskStateSimulator


@pytest.fixture
def test_dag() -> DAG:
    """Provide a clean DAG instance for testing.

    :returns: A new DAG instance with default settings
    """
    dag = DAG(dag_id="test_dag", start_date=datetime.now())
    return dag


@pytest.fixture
def mock_operator_fixture() -> MockOperator:
    """Provide a pre-configured mock operator.

    :returns: A MockOperator instance
    """
    return MockOperator(task_id="test_task", return_value={"status": "success"})


@pytest.fixture
def test_context() -> dict[str, Any]:
    """Provide a test execution context.

    :returns: Dictionary with common test context values
    """
    execution_date = datetime.now()
    return {
        "execution_date": execution_date,
        "ds": execution_date.strftime("%Y-%m-%d"),
        "ds_nodash": execution_date.strftime("%Y%m%d"),
        "ts": execution_date.isoformat(),
        "ts_nodash": execution_date.strftime("%Y%m%dT%H%M%S"),
    }


@pytest.fixture
def task_instance_factory(test_context: dict[str, Any]):
    """Provide a factory for creating task instances.

    :returns: A factory function for creating task instances
    """
    simulator = TaskStateSimulator()

    def _create_task_instance(
        task: Operator,
        state: str = "queued",
        **kwargs: Any,
    ):
        return simulator.create_task_instance(
            task=task,
            state=state,
            execution_date=test_context["execution_date"],
            **kwargs,
        )

    return _create_task_instance


@pytest.fixture
def dag_with_tasks(test_dag: DAG) -> DAG:
    """Provide a DAG with some sample tasks for testing.

    :returns: A DAG with sample tasks
    """
    from airflow.operators.empty import EmptyOperator

    with test_dag:
        task1 = EmptyOperator(task_id="task_1")
        task2 = EmptyOperator(task_id="task_2")
        task3 = EmptyOperator(task_id="task_3")

        task1 >> task2 >> task3

    return test_dag
