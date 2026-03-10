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

from airflow.sdk import DAG
from airflow.sdk.definitions.context import Context

from tests_common.test_utils.dag import (
    assert_dag_dependencies,
    assert_dag_has_no_cycles,
    assert_dag_has_single_root,
    assert_dag_has_tasks,
    assert_dag_is_connected,
    assert_task_downstream_of,
    assert_task_in_dag,
    assert_task_not_in_dag,
    assert_task_upstream_of,
    get_all_downstream_ids,
    get_all_upstream_ids,
    get_dag_leaves,
    get_dag_level_order,
    get_dag_roots,
    get_dag_structure_dict,
    get_dag_task_ids,
    get_dag_tasks,
    get_task_depth,
    get_task_downstream_ids,
    get_task_upstream_ids,
)
from tests_common.test_utils.mock_operators import (
    MockBranchOperator,
    MockFailOperator,
    MockIncrementOperator,
    MockPythonOperator,
    MockSensorOperator,
    MockSKippableOperator,
    MockSucceedOperator,
    MockSensorsListOperator,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def simple_dag():
    """Create a simple DAG for testing."""
    dag = DAG(dag_id="test_dag", schedule=None)

    from airflow.providers.standard.operators.empty import EmptyOperator

    task1 = EmptyOperator(task_id="task1", dag=dag)
    task2 = EmptyOperator(task_id="task2", dag=dag)
    task3 = EmptyOperator(task_id="task3", dag=dag)

    task1 >> task2 >> task3

    return dag


@pytest.fixture
def branching_dag():
    """Create a branching DAG for testing."""
    dag = DAG(dag_id="branching_dag", schedule=None)

    from airflow.providers.standard.operators.empty import EmptyOperator

    start = EmptyOperator(task_id="start", dag=dag)
    branch_a = EmptyOperator(task_id="branch_a", dag=dag)
    branch_b = EmptyOperator(task_id="branch_b", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    start >> [branch_a, branch_b] >> end

    return dag


@pytest.fixture
def diamond_dag():
    """Create a diamond-shaped DAG for testing."""
    dag = DAG(dag_id="diamond_dag", schedule=None)

    from airflow.providers.standard.operators.empty import EmptyOperator

    start = EmptyOperator(task_id="start", dag=dag)
    branch_a = EmptyOperator(task_id="branch_a", dag=dag)
    branch_b = EmptyOperator(task_id="branch_b", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    start >> branch_a >> end
    start >> branch_b >> end

    return dag


# =============================================================================
# Tests for Mock Operators
# =============================================================================


class TestMockSensorOperator:
    """Tests for MockSensorOperator."""

    def test_sensor_returns_poke_result_true(self):
        """Test sensor returns True when poke_result is True."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockSensorOperator(task_id="test_sensor", poke_result=True)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result is True

    def test_sensor_returns_poke_result_false(self):
        """Test sensor returns False when poke_result is False."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockSensorOperator(task_id="test_sensor", poke_result=False)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result is False


class TestMockBranchOperator:
    """Tests for MockBranchOperator."""

    def test_branch_returns_string(self):
        """Test branch returns the configured string."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockBranchOperator(task_id="test_branch", branches="branch_a")
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "branch_a"

    def test_branch_returns_first_from_list(self):
        """Test branch returns first item from list."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockBranchOperator(task_id="test_branch", branches=["branch_a", "branch_b"])
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "branch_a"

    def test_branch_returns_none_when_no_branches(self):
        """Test branch returns None when no branches configured."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockBranchOperator(task_id="test_branch", branches=None)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result is None


class TestMockFailOperator:
    """Tests for MockFailOperator."""

    def test_fail_operator_raises_exception(self):
        """Test fail operator raises exception when fail_at_execute is True."""
        from tests_common.test_utils.mock_context import mock_context
        from airflow.exceptions import AirflowException

        operator = MockFailOperator(task_id="fail_task", fail_at_execute=True)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        with pytest.raises(AirflowException, match="Task failed"):
            operator.execute(context)

    def test_fail_operator_returns_value_when_no_fail(self):
        """Test fail operator returns value when fail_at_execute is False."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockFailOperator(
            task_id="fail_task", fail_at_execute=False, return_value="success"
        )
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "success"


class TestMockPythonOperator:
    """Tests for MockPythonOperator."""

    def test_python_operator_calls_callable(self):
        """Test python operator calls the callable."""
        from tests_common.test_utils.mock_context import mock_context

        called = []

        def my_callable(context):
            called.append(context)
            return "called"

        operator = MockPythonOperator(task_id="python_task", python_callable=my_callable)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "called"
        assert len(called) == 1

    def test_python_operator_returns_value(self):
        """Test python operator returns configured value when no callable."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockPythonOperator(task_id="python_task", return_value="my_value")
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "my_value"


class TestMockSucceedOperator:
    """Tests for MockSucceedOperator."""

    def test_succeed_operator_returns_value(self):
        """Test succeed operator returns configured value."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockSucceedOperator(task_id="succeed_task", return_value="done")
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "done"


class TestMockIncrementOperator:
    """Tests for MockIncrementOperator."""

    def test_increment_operator(self):
        """Test increment operator increments counter."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockIncrementOperator(task_id="increment_task", increment_by=1)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == 1

        # Check XCom was pushed
        xcom_value = context["ti"].xcom_pull(key="counter", default=0)
        assert xcom_value == 1


class TestMockSKippableOperator:
    """Tests for MockSKippableOperator."""

    def test_skip_operator_skips(self):
        """Test skip operator raises skip exception when should_skip is True."""
        from tests_common.test_utils.mock_context import mock_context
        from tests_common.test_utils.mock_operators import AirflowSkipException

        if AirflowSkipException is None:
            pytest.skip("AirflowSkipException not available in this version")

        operator = MockSKippableOperator(task_id="skip_task", should_skip=True)
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        with pytest.raises(AirflowSkipException):
            operator.execute(context)

    def test_skip_operator_executes(self):
        """Test skip operator executes when should_skip is False."""
        from tests_common.test_utils.mock_context import mock_context

        operator = MockSKippableOperator(
            task_id="skip_task", should_skip=False, return_value="executed"
        )
        dag = DAG(dag_id="test", schedule=None)
        dag.add_task(operator)
        context = mock_context(operator)

        result = operator.execute(context)

        assert result == "executed"


# =============================================================================
# Tests for DAG Structure Helpers
# =============================================================================


class TestGetDagTasks:
    """Tests for get_dag_tasks function."""

    def test_get_dag_tasks(self, simple_dag):
        """Test getting all tasks from a DAG."""
        tasks = get_dag_tasks(simple_dag)

        assert len(tasks) == 3
        assert "task1" in tasks
        assert "task2" in tasks
        assert "task3" in tasks


class TestGetDagTaskIds:
    """Tests for get_dag_task_ids function."""

    def test_get_dag_task_ids(self, simple_dag):
        """Test getting all task IDs from a DAG."""
        task_ids = get_dag_task_ids(simple_dag)

        assert task_ids == {"task1", "task2", "task3"}


class TestGetTaskUpstreamIds:
    """Tests for get_task_upstream_ids function."""

    def test_get_upstream_ids(self, simple_dag):
        """Test getting upstream task IDs."""
        upstream = get_task_upstream_ids(simple_dag, "task2")

        assert upstream == {"task1"}

    def test_get_upstream_ids_root_task(self, simple_dag):
        """Test getting upstream IDs for root task."""
        upstream = get_task_upstream_ids(simple_dag, "task1")

        assert upstream == set()


class TestGetTaskDownstreamIds:
    """Tests for get_task_downstream_ids function."""

    def test_get_downstream_ids(self, simple_dag):
        """Test getting downstream task IDs."""
        downstream = get_task_downstream_ids(simple_dag, "task1")

        assert downstream == {"task2"}

    def test_get_downstream_ids_leaf_task(self, simple_dag):
        """Test getting downstream IDs for leaf task."""
        downstream = get_task_downstream_ids(simple_dag, "task3")

        assert downstream == set()


class TestGetAllUpstreamIds:
    """Tests for get_all_upstream_ids function."""

    def test_get_all_upstream_ids(self, diamond_dag):
        """Test getting all upstream IDs transitively."""
        all_upstream = get_all_upstream_ids(diamond_dag, "end")

        assert all_upstream == {"start", "branch_a", "branch_b"}


class TestGetAllDownstreamIds:
    """Tests for get_all_downstream_ids function."""

    def test_get_all_downstream_ids(self, diamond_dag):
        """Test getting all downstream IDs transitively."""
        all_downstream = get_all_downstream_ids(diamond_dag, "start")

        assert all_downstream == {"branch_a", "branch_b", "end"}


class TestGetDagRoots:
    """Tests for get_dag_roots function."""

    def test_get_dag_roots(self, simple_dag):
        """Test getting root tasks."""
        roots = get_dag_roots(simple_dag)

        assert roots == ["task1"]

    def test_get_dag_roots_branching(self, branching_dag):
        """Test getting root tasks in branching DAG."""
        roots = get_dag_roots(branching_dag)

        assert roots == ["start"]


class TestGetDagLeaves:
    """Tests for get_dag_leaves function."""

    def test_get_dag_leaves(self, simple_dag):
        """Test getting leaf tasks."""
        leaves = get_dag_leaves(simple_dag)

        assert leaves == ["task3"]

    def test_get_dag_leaves_diamond(self, diamond_dag):
        """Test getting leaf tasks in diamond DAG."""
        leaves = get_dag_leaves(diamond_dag)

        assert leaves == ["end"]


class TestGetTaskDepth:
    """Tests for get_task_depth function."""

    def test_get_task_depth(self, simple_dag):
        """Test getting task depth."""
        depth = get_task_depth(simple_dag, "task1")
        assert depth == 0

        depth = get_task_depth(simple_dag, "task2")
        assert depth == 1

        depth = get_task_depth(simple_dag, "task3")
        assert depth == 2


# =============================================================================
# Tests for Assertion Helpers
# =============================================================================


class TestAssertDagHasTasks:
    """Tests for assert_dag_has_tasks function."""

    def test_assert_dag_has_tasks_pass(self, simple_dag):
        """Test assertion passes when tasks exist."""
        assert_dag_has_tasks(simple_dag, ["task1", "task2", "task3"])

    def test_assert_dag_has_tasks_fail(self, simple_dag):
        """Test assertion fails when tasks are missing."""
        with pytest.raises(AssertionError, match="missing expected tasks"):
            assert_dag_has_tasks(simple_dag, ["task1", "nonexistent"])


class TestAssertTaskInDag:
    """Tests for assert_task_in_dag function."""

    def test_assert_task_in_dag_pass(self, simple_dag):
        """Test assertion passes when task exists."""
        assert_task_in_dag(simple_dag, "task1")

    def test_assert_task_in_dag_fail(self, simple_dag):
        """Test assertion fails when task doesn't exist."""
        with pytest.raises(AssertionError, match="not found"):
            assert_task_in_dag(simple_dag, "nonexistent")


class TestAssertTaskNotInDag:
    """Tests for assert_task_not_in_dag function."""

    def test_assert_task_not_in_dag_pass(self, simple_dag):
        """Test assertion passes when task doesn't exist."""
        assert_task_not_in_dag(simple_dag, "nonexistent")

    def test_assert_task_not_in_dag_fail(self, simple_dag):
        """Test assertion fails when task exists."""
        with pytest.raises(AssertionError, match="should not exist"):
            assert_task_not_in_dag(simple_dag, "task1")


class TestAssertDagDependencies:
    """Tests for assert_dag_dependencies function."""

    def test_assert_upstream_dependencies(self, simple_dag):
        """Test assertion for upstream dependencies."""
        assert_dag_dependencies(simple_dag, "task2", upstream=["task1"])

    def test_assert_downstream_dependencies(self, simple_dag):
        """Test assertion for downstream dependencies."""
        assert_dag_dependencies(simple_dag, "task1", downstream=["task2"])

    def test_assert_dependencies_fail(self, simple_dag):
        """Test assertion fails when dependencies don't match."""
        with pytest.raises(AssertionError, match="upstream.*mismatch"):
            assert_dag_dependencies(simple_dag, "task2", upstream=["wrong_task"])


class TestAssertDagHasSingleRoot:
    """Tests for assert_dag_has_single_root function."""

    def test_assert_single_root_pass(self, simple_dag):
        """Test assertion passes for DAG with single root."""
        assert_dag_has_single_root(simple_dag)

    def test_assert_single_root_fail(self, diamond_dag):
        """Test assertion fails for DAG with multiple roots."""
        with pytest.raises(AssertionError, match="exactly one root"):
            assert_dag_has_single_root(diamond_dag)


class TestAssertDagHasNoCycles:
    """Tests for assert_dag_has_no_cycles function."""

    def test_assert_no_cycles_pass(self, simple_dag):
        """Test assertion passes for acyclic DAG."""
        assert_dag_has_no_cycles(simple_dag)


class TestAssertDagIsConnected:
    """Tests for assert_dag_is_connected function."""

    def test_assert_connected_pass(self, simple_dag):
        """Test assertion passes for connected DAG."""
        assert_dag_is_connected(simple_dag)

    def test_assert_connected_pass_diamond(self, diamond_dag):
        """Test assertion passes for connected diamond DAG."""
        assert_dag_is_connected(diamond_dag)


class TestAssertTaskUpstreamOf:
    """Tests for assert_task_upstream_of function."""

    def test_assert_upstream_pass(self, simple_dag):
        """Test assertion passes when upstream relationship exists."""
        assert_task_upstream_of(simple_dag, "task1", "task2")

    def test_assert_upstream_fail(self, simple_dag):
        """Test assertion fails when upstream relationship doesn't exist."""
        with pytest.raises(AssertionError, match="not upstream of"):
            assert_task_upstream_of(simple_dag, "task2", "task1")


class TestAssertTaskDownstreamOf:
    """Tests for assert_task_downstream_of function."""

    def test_assert_downstream_pass(self, simple_dag):
        """Test assertion passes when downstream relationship exists."""
        assert_task_downstream_of(simple_dag, "task2", "task1")

    def test_assert_downstream_fail(self, simple_dag):
        """Test assertion fails when downstream relationship doesn't exist."""
        with pytest.raises(AssertionError, match="not upstream of"):
            assert_task_downstream_of(simple_dag, "task1", "task2")


class TestGetDagStructureDict:
    """Tests for get_dag_structure_dict function."""

    def test_get_structure_dict(self, simple_dag):
        """Test getting DAG structure as dictionary."""
        structure = get_dag_structure_dict(simple_dag)

        assert structure["task1"] == ["task2"]
        assert structure["task2"] == ["task3"]
        assert structure["task3"] == []


class TestGetDagLevelOrder:
    """Tests for get_dag_level_order function."""

    def test_get_level_order(self, diamond_dag):
        """Test getting DAG level order."""
        levels = get_dag_level_order(diamond_dag)

        assert levels[0] == ["start"]
        assert set(levels[1]) == {"branch_a", "branch_b"}
        assert levels[2] == ["end"]
