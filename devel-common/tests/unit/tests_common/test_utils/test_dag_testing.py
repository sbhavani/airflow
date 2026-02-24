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
"""Tests for DAG testing utilities."""
from __future__ import annotations

import pytest

from tests_common.test_utils.dag_testing import (
    BranchingMockOperator,
    CallbackMockOperator,
    EmptyMockOperator,
    ParameterizedMockOperator,
    SensorMockOperator,
    TaskState,
    assert_no_cycles,
    assert_no_orphans,
    assert_task_depends_on,
    assert_task_exists,
    assert_task_order,
    assert_valid_dag,
    get_all_tasks,
    get_direct_ancestors,
    get_direct_descendants,
    get_task_by_id,
    simulate_task_states,
)


class TestMockOperators:
    """Test mock operators."""

    def test_empty_mock_operator(self, dag_maker):
        """Test EmptyMockOperator basic execution."""
        with dag_maker("test_dag") as dag:
            task = EmptyMockOperator(task_id="test_task")

        assert task is not None
        assert task.task_id == "test_task"
        assert task.dag_id == "test_dag"

    def test_parameterized_mock_operator_with_return_value(self, dag_maker):
        """Test ParameterizedMockOperator with return value."""
        with dag_maker("test_dag") as dag:
            task = ParameterizedMockOperator(
                task_id="test_task",
                return_value="test_result",
            )

        result = task.execute({})
        assert result == "test_result"

    def test_parameterized_mock_operator_with_side_effect(self, dag_maker):
        """Test ParameterizedMockOperator with side effect."""
        with dag_maker("test_dag") as dag:
            call_count = {"count": 0}

            def side_effect():
                call_count["count"] += 1
                return "side_effect_result"

            task = ParameterizedMockOperator(
                task_id="test_task",
                side_effect=side_effect,
            )

        result = task.execute({})
        assert result == "side_effect_result"
        assert call_count["count"] == 1

    def test_parameterized_mock_operator_with_xcom_push(self, dag_maker):
        """Test ParameterizedMockOperator XCom push."""
        from unittest.mock import MagicMock

        with dag_maker("test_dag") as dag:
            task = ParameterizedMockOperator(
                task_id="test_task",
                return_value="xcom_value",
                push_xcom_key="result_key",
            )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}
        task.execute(context)
        mock_ti.xcom_push.assert_called_once_with(key="result_key", value="xcom_value")

    def test_parameterized_mock_operator_fail_at_execution(self, dag_maker):
        """Test ParameterizedMockOperator fail at execution."""
        with dag_maker("test_dag") as dag:
            task = ParameterizedMockOperator(
                task_id="test_task",
                fail_at_execution=True,
            )

        with pytest.raises(RuntimeError, match="Task test_task failed as configured"):
            task.execute({})

    def test_branching_mock_operator(self, dag_maker):
        """Test BranchingMockOperator."""
        with dag_maker("test_dag") as dag:
            task = BranchingMockOperator(
                task_id="branch_task",
                branches=["task_a", "task_b"],
                selected_branch="task_a",
            )

        result = task.execute({})
        assert result == "task_a"

    def test_branching_mock_operator_invalid_branch(self, dag_maker):
        """Test BranchingMockOperator with invalid branch."""
        with dag_maker("test_dag") as dag:
            task = BranchingMockOperator(
                task_id="branch_task",
                branches=["task_a", "task_b"],
                selected_branch="invalid_task",
            )

        with pytest.raises(ValueError, match="Selected branch 'invalid_task' not in available branches"):
            task.execute({})

    def test_callback_mock_operator(self, dag_maker):
        """Test CallbackMockOperator records callbacks."""
        callbacks_tracker = {}

        with dag_maker("test_dag") as dag:
            task = CallbackMockOperator(
                task_id="callback_task",
                callbacks_tracker=callbacks_tracker,
            )

        task.execute({})

        assert "callback_task" in callbacks_tracker
        assert callbacks_tracker["callback_task"][0]["callback"] == "execute"

    def test_sensor_mock_operator_success(self, dag_maker):
        """Test SensorMockOperator succeeds."""
        with dag_maker("test_dag") as dag:
            task = SensorMockOperator(
                task_id="sensor_task",
                poke_return_value=True,
            )

        result = task.execute({})
        assert result is True

    def test_sensor_mock_operator_fail(self, dag_maker):
        """Test SensorMockOperator fails."""
        with dag_maker("test_dag") as dag:
            task = SensorMockOperator(
                task_id="sensor_task",
                should_fail=True,
            )

        with pytest.raises(RuntimeError, match="Sensor sensor_task failed"):
            task.execute({})


class TestDAGAssertions:
    """Test DAG assertion helpers."""

    def test_assert_valid_dag(self, dag_maker):
        """Test assert_valid_dag passes for valid DAG."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        assert_valid_dag(dag)  # Should not raise

    def test_assert_valid_dag_no_tasks(self):
        """Test assert_valid_dag fails for DAG without tasks."""
        from airflow.sdk import DAG

        dag = DAG(dag_id="empty_dag", start_date=None)
        with pytest.raises(AssertionError, match="DAG must have at least one task"):
            assert_valid_dag(dag)

    def test_assert_no_cycles(self, dag_maker):
        """Test assert_no_cycles passes for DAG without cycles."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task3 = EmptyMockOperator(task_id="task3")
            task1 >> task2 >> task3

        assert_no_cycles(dag)  # Should not raise

    def test_assert_no_cycles_detects_cycle(self, dag_maker):
        """Test assert_no_cycles detects cycles."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2
            task2 >> task1  # Creates a cycle

        with pytest.raises(AssertionError, match="Cycle detected"):
            assert_no_cycles(dag)

    def test_assert_task_exists(self, dag_maker):
        """Test assert_task_exists."""
        with dag_maker("test_dag") as dag:
            task = EmptyMockOperator(task_id="existing_task")

        assert_task_exists(dag, "existing_task")  # Should not raise
        with pytest.raises(AssertionError, match="Task 'nonexistent' not found"):
            assert_task_exists(dag, "nonexistent")

    def test_assert_task_depends_on(self, dag_maker):
        """Test assert_task_depends_on."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        assert_task_depends_on(dag, "task2", ["task1"])  # Should not raise
        with pytest.raises(AssertionError, match="upstream dependencies mismatch"):
            assert_task_depends_on(dag, "task1", ["task2"])

    def test_assert_task_order(self, dag_maker):
        """Test assert_task_order."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task3 = EmptyMockOperator(task_id="task3")
            task1 >> task2 >> task3

        assert_task_order(dag, ["task1", "task2", "task3"])  # Should not raise
        with pytest.raises(AssertionError, match="is not downstream of"):
            assert_task_order(dag, ["task2", "task1", "task3"])

    def test_assert_no_orphans(self, dag_maker):
        """Test assert_no_orphans."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        assert_no_orphans(dag)  # Should not raise

    def test_assert_no_orphans_detects_orphan(self, dag_maker):
        """Test assert_no_orphans detects orphan tasks."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            # task2 has no connections

        with pytest.raises(AssertionError, match="orphan tasks"):
            assert_no_orphans(dag)


class TestDAGTraversal:
    """Test DAG traversal helpers."""

    def test_get_task_by_id(self, dag_maker):
        """Test get_task_by_id."""
        with dag_maker("test_dag") as dag:
            task = EmptyMockOperator(task_id="test_task")

        result = get_task_by_id(dag, "test_task")
        assert result is not None
        assert result.task_id == "test_task"

        assert get_task_by_id(dag, "nonexistent") is None

    def test_get_all_tasks(self, dag_maker):
        """Test get_all_tasks."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")

        tasks = get_all_tasks(dag)
        assert len(tasks) == 2
        task_ids = {t.task_id for t in tasks}
        assert task_ids == {"task1", "task2"}

    def test_get_direct_ancestors(self, dag_maker):
        """Test get_direct_ancestors."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        ancestors = get_direct_ancestors(task2)
        assert len(ancestors) == 1
        assert ancestors[0].task_id == "task1"

    def test_get_direct_descendants(self, dag_maker):
        """Test get_direct_descendants."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        descendants = get_direct_descendants(task1)
        assert len(descendants) == 1
        assert descendants[0].task_id == "task2"


class TestTaskLifecycle:
    """Test task lifecycle simulation."""

    def test_simulate_task_states(self, dag_maker):
        """Test simulate_task_states creates task instances with correct states."""
        with dag_maker("test_dag") as dag:
            task1 = EmptyMockOperator(task_id="task1")
            task2 = EmptyMockOperator(task_id="task2")
            task1 >> task2

        task_instances = simulate_task_states(
            dag,
            {
                "task1": TaskState.SUCCESS,
                "task2": TaskState.RUNNING,
            },
        )

        assert len(task_instances) == 2
        assert task_instances["task1"].state == TaskState.SUCCESS.value
        assert task_instances["task2"].state == TaskState.RUNNING.value

    def test_simulate_task_states_invalid_task(self, dag_maker):
        """Test simulate_task_states raises for invalid task."""
        with dag_maker("test_dag") as dag:
            task = EmptyMockOperator(task_id="task1")

        with pytest.raises(ValueError, match="Task 'nonexistent' not found"):
            simulate_task_states(dag, {"nonexistent": TaskState.SUCCESS})

    def test_task_state_enum_values(self):
        """Test TaskState enum has correct values."""
        assert TaskState.SUCCESS.value == "success"
        assert TaskState.FAILED.value == "failed"
        assert TaskState.RUNNING.value == "running"
        assert TaskState.QUEUED.value == "queued"
        assert TaskState.SKIPPED.value == "skipped"
