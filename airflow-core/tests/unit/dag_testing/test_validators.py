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
"""Unit tests for DAG validators."""

from __future__ import annotations

import pytest

from airflow.dag_testing.validators import (
    assert_no_circular_dependencies,
    assert_task_depends_on,
    assert_task_exists,
    assert_task_has_downstream,
    assert_task_naming_convention,
    get_dag_structure,
)
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


@pytest.fixture
def simple_dag():
    """Create a simple DAG for testing."""
    dag = DAG(dag_id="test_dag", start_date=None)
    with dag:
        task1 = EmptyOperator(task_id="task_1")
        task2 = EmptyOperator(task_id="task_2")
        task3 = EmptyOperator(task_id="task_3")

        task1 >> task2 >> task3
    return dag


@pytest.fixture
def complex_dag():
    """Create a more complex DAG with multiple branches."""
    dag = DAG(dag_id="complex_dag", start_date=None)
    with dag:
        start = EmptyOperator(task_id="start")
        branch_a = EmptyOperator(task_id="branch_a")
        branch_b = EmptyOperator(task_id="branch_b")
        end = EmptyOperator(task_id="end")

        start >> [branch_a, branch_b]
        branch_a >> end
        branch_b >> end
    return dag


class TestAssertTaskExists:
    """Tests for assert_task_exists."""

    def test_task_exists(self, simple_dag):
        """Test that assertion passes for existing task."""
        assert_task_exists(simple_dag, "task_1")

    def test_task_not_exists(self, simple_dag):
        """Test that assertion fails for non-existent task."""
        with pytest.raises(AssertionError, match="does not exist"):
            assert_task_exists(simple_dag, "non_existent_task")


class TestAssertTaskDependsOn:
    """Tests for assert_task_depends_on."""

    def test_correct_upstream(self, simple_dag):
        """Test that assertion passes with correct dependencies."""
        assert_task_depends_on(simple_dag, "task_2", ["task_1"])
        assert_task_depends_on(simple_dag, "task_3", ["task_2"])

    def test_incorrect_upstream(self, simple_dag):
        """Test that assertion fails with incorrect dependencies."""
        with pytest.raises(AssertionError, match="upstream dependencies mismatch"):
            assert_task_depends_on(simple_dag, "task_2", ["task_3"])

    def test_multiple_upstream(self, complex_dag):
        """Test with multiple upstream dependencies."""
        assert_task_depends_on(complex_dag, "end", ["branch_a", "branch_b"])


class TestAssertTaskHasDownstream:
    """Tests for assert_task_has_downstream."""

    def test_correct_downstream(self, simple_dag):
        """Test that assertion passes with correct downstream."""
        assert_task_has_downstream(simple_dag, "task_1", ["task_2"])

    def test_multiple_downstream(self, complex_dag):
        """Test with multiple downstream dependencies."""
        assert_task_has_downstream(complex_dag, "start", ["branch_a", "branch_b"])

    def test_incorrect_downstream(self, simple_dag):
        """Test that assertion fails with incorrect downstream."""
        with pytest.raises(AssertionError, match="downstream dependencies mismatch"):
            assert_task_has_downstream(simple_dag, "task_1", ["task_3"])


class TestAssertNoCircularDependencies:
    """Tests for assert_no_circular_dependencies."""

    def test_no_cycle(self, simple_dag):
        """Test that DAG without cycles passes."""
        assert_no_circular_dependencies(simple_dag)

    def test_with_cycle(self):
        """Test that DAG with cycles fails."""
        dag = DAG(dag_id="cycle_dag", start_date=None)
        with dag:
            task1 = EmptyOperator(task_id="task_1")
            task2 = EmptyOperator(task_id="task_2")
            task1 >> task2
            task2 >> task1  # Creates a cycle

        with pytest.raises(AssertionError, match="Circular dependency"):
            assert_no_circular_dependencies(dag)


class TestAssertTaskNamingConvention:
    """Tests for assert_task_naming_convention."""

    def test_valid_names(self, simple_dag):
        """Test that valid naming pattern passes."""
        # Pattern that matches task_1, task_2, etc.
        assert_task_naming_convention(simple_dag, r"^task_\d+$")

    def test_invalid_names(self, simple_dag):
        """Test that invalid naming pattern fails."""
        # Add a task with invalid name
        from airflow.operators.python import PythonOperator

        with simple_dag:
            PythonOperator(
                task_id="invalid-name",
                python_callable=lambda: None,
            )

        with pytest.raises(AssertionError, match="do not match naming convention"):
            assert_task_naming_convention(simple_dag, r"^task_\d+$")


class TestGetDagStructure:
    """Tests for get_dag_structure."""

    def test_get_structure(self, complex_dag):
        """Test that DAG structure is correctly extracted."""
        structure = get_dag_structure(complex_dag)

        assert structure["dag_id"] == "complex_dag"
        assert "start" in structure["tasks"]
        assert "branch_a" in structure["tasks"]
        assert "branch_b" in structure["tasks"]
        assert "end" in structure["tasks"]

        # Check dependencies
        assert "start" in structure["dependencies"]
        assert set(structure["dependencies"]["end"]["upstream"]) == {"branch_a", "branch_b"}

    def test_empty_dag(self):
        """Test with an empty DAG."""
        dag = DAG(dag_id="empty_dag", start_date=None)
        structure = get_dag_structure(dag)

        assert structure["dag_id"] == "empty_dag"
        assert structure["tasks"] == []
        assert structure["dependencies"] == {}
