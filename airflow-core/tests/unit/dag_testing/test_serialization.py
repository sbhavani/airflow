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
"""Unit tests for DAG serialization utilities."""

from __future__ import annotations

import pytest

from airflow.dag_testing.serialization import (
    get_serialization_errors,
    validate_dag_serialization,
)
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


@pytest.fixture
def valid_dag():
    """Create a valid DAG for testing."""
    dag = DAG(dag_id="valid_dag", start_date=None)
    with dag:
        task1 = EmptyOperator(task_id="task_1")
        task2 = EmptyOperator(task_id="task_2")
        task1 >> task2
    return dag


@pytest.fixture
def dag_with_duplicate_tasks():
    """Create a DAG with duplicate task IDs."""
    dag = DAG(dag_id="duplicate_dag", start_date=None)
    with dag:
        # Create tasks that will have duplicate IDs
        EmptyOperator(task_id="duplicate_task")
        EmptyOperator(task_id="duplicate_task")
    return dag


class TestValidateDagSerialization:
    """Tests for validate_dag_serialization."""

    def test_valid_dag_passes(self, valid_dag):
        """Test that valid DAG passes validation."""
        errors = validate_dag_serialization(valid_dag)
        assert errors == []

    def test_duplicate_task_ids(self, dag_with_duplicate_tasks):
        """Test that duplicate task IDs are detected."""
        errors = validate_dag_serialization(dag_with_duplicate_tasks)
        assert len(errors) > 0
        assert any("Duplicate" in err for err in errors)


class TestGetSerializationErrors:
    """Tests for get_serialization_errors."""

    def test_valid_dag_no_errors(self, valid_dag):
        """Test that valid DAG has no errors."""
        result = get_serialization_errors(valid_dag)
        assert result["valid"] is True
        assert result["errors"] == []
        assert result["task_errors"] == {}

    def test_dag_with_duplicate_tasks(self, dag_with_duplicate_tasks):
        """Test that duplicate tasks are detected."""
        result = get_serialization_errors(dag_with_duplicate_tasks)
        assert result["valid"] is False
        assert "Duplicate" in str(result["errors"])
