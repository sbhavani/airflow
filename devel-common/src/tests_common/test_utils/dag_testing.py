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
"""Testing utilities for DAG unit testing.

This module provides helper functions for DAG unit testing including:
- Mock operators for testing
- Task lifecycle simulation
- Assertion helpers for DAG structure validation
"""
from __future__ import annotations

import itertools
from collections.abc import Callable, Iterable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.models import TaskInstance
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.sdk import DAG, BaseOperator
    from airflow.sdk.types import Operator

    from tests_common.test_utils.pytest_plugin import DagMaker

__all__ = [
    "ParameterizedMockOperator",
    "BranchingMockOperator",
    "CallbackMockOperator",
    "SensorMockOperator",
    "EmptyMockOperator",
    "TaskState",
    "simulate_task_states",
    "simulate_dag_run",
    "assert_valid_dag",
    "assert_task_order",
    "assert_no_cycles",
    "assert_task_exists",
    "assert_task_depends_on",
    "assert_no_orphans",
    "get_task_by_id",
    "get_all_tasks",
    "get_direct_ancestors",
    "get_direct_descendants",
]


# Task state enum for simulation
class TaskState(Enum):
    """Task instance states for simulation."""

    QUEUED = TaskInstanceState.QUEUED
    RUNNING = TaskInstanceState.RUNNING
    SUCCESS = TaskInstanceState.SUCCESS
    FAILED = TaskInstanceState.FAILED
    SKIPPED = TaskInstanceState.SKIPPED
    UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED
    UP_FOR_RETRY = TaskInstanceState.UP_FOR_RETRY
    UP_FOR_RESCHEDULE = TaskInstanceState.UP_FOR_RESCHEDULE


class EmptyMockOperator(BaseOperator):
    """Simple mock operator that does nothing.

    This is a basic operator for testing that accepts no special arguments
    and performs no operation during execution.
    """

    def execute(self, context: "Operator"):
        pass


class ParameterizedMockOperator(BaseOperator):
    """Mock operator that can be parameterized for testing.

    This operator allows you to specify custom behavior via constructor arguments.
    Useful for testing different operator configurations without creating
    multiple operator classes.

    Args:
        return_value: Value to return from execute (default: None)
        side_effect: Optional callable to execute instead of default behavior
        push_xcom_key: Optional XCom key to push return_value to
        fail_at_execution: If True, raises an exception during execute
    """

    template_fields: Sequence[str] = ("arg1", "arg2")

    def __init__(
        self,
        *,
        return_value: Any = None,
        side_effect: Callable[[], Any] | None = None,
        push_xcom_key: str | None = None,
        fail_at_execution: bool = False,
        arg1: str = "",
        arg2: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.return_value = return_value
        self.side_effect = side_effect
        self.push_xcom_key = push_xcom_key
        self.fail_at_execution = fail_at_execution
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context: "Operator"):
        if self.fail_at_execution:
            raise RuntimeError(f"Task {self.task_id} failed as configured")

        if self.side_effect is not None:
            result = self.side_effect()
        else:
            result = self.return_value

        if self.push_xcom_key is not None:
            context["ti"].xcom_push(key=self.push_xcom_key, value=result)

        return result


class BranchingMockOperator(BaseOperator):
    """Mock operator for testing branching logic.

    This operator selects one of multiple downstream tasks based on a
    condition. Useful for testing conditional workflow paths.

    Args:
        branches: List of task_ids that can be selected
        selected_branch: The task_id to select (must be in branches)
    """

    template_fields: Sequence[str] = ("selected_branch",)

    def __init__(self, *, branches: list[str] | None = None, selected_branch: str = "", **kwargs):
        super().__init__(**kwargs)
        self.branches = branches or []
        self.selected_branch = selected_branch

    def execute(self, context: "Operator") -> str:
        """Return the selected branch task_id."""
        if self.selected_branch and self.branches and self.selected_branch not in self.branches:
            raise ValueError(
                f"Selected branch '{self.selected_branch}' not in available branches: {self.branches}"
            )
        return self.selected_branch


class CallbackMockOperator(BaseOperator):
    """Mock operator that records callback invocations.

    Useful for testing that Airflow callbacks (on_success_callback,
    on_failure_callback, etc.) are properly triggered.

    Args:
        callbacks_tracker: Dictionary to store callback invocation info
        callback_type: Which callback this operator should record
    """

    def __init__(
        self,
        *,
        callbacks_tracker: dict[str, list[dict[str, Any]]] | None = None,
        callback_type: str = "execute",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.callbacks_tracker = callbacks_tracker or {}
        self.callback_type = callback_type

    def execute(self, context: "Operator"):
        self._record_callback("execute")
        return None

    def on_success(self, context: "Operator"):
        self._record_callback("on_success")

    def on_failure(self, context: "Operator", exception: Exception):
        self._record_callback("on_failure", {"exception": exception})

    def on_retry(self, context: "Operator"):
        self._record_callback("on_retry")

    def _record_callback(self, callback_name: str, extra_data: dict | None = None):
        if self.task_id not in self.callbacks_tracker:
            self.callbacks_tracker[self.task_id] = []
        self.callbacks_tracker[self.task_id].append(
            {
                "callback": callback_name,
                "dag_id": self.dag_id,
                "extra": extra_data or {},
            }
        )


class SensorMockOperator(BaseOperator):
    """Mock sensor operator for testing.

    Simulates a sensor that can either poke (succeed) or defer (wait).
    Useful for testing sensor behavior without external dependencies.

    Args:
        poke_return_value: Value returned by poke() method (default: True)
        should_fail: If True, the sensor will fail instead of succeeding
        poke_count: Number of pokes before returning poke_return_value
    """

    def __init__(
        self,
        *,
        poke_return_value: bool = True,
        should_fail: bool = False,
        poke_count: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.poke_return_value = poke_return_value
        self.should_fail = should_fail
        self.poke_count = poke_count
        self._current_poke = 0

    def execute(self, context: "Operator") -> bool:
        """Execute the sensor logic."""
        if self.should_fail:
            raise RuntimeError(f"Sensor {self.task_id} failed")
        return self.poke_return_value

    def poke(self, context: "Operator") -> bool:
        """Poke the sensor to check if conditions are met."""
        self._current_poke += 1
        if self.poke_count > 0 and self._current_poke <= self.poke_count:
            return False
        return self.poke_return_value


# Task lifecycle simulation functions


def simulate_task_states(
    dag: "DAG",
    task_states: dict[str, TaskState],
    dag_run_id: str | None = None,
) -> dict[str, TaskInstance]:
    """Simulate task instances with specific states.

    Args:
        dag: The DAG to simulate tasks for
        task_states: Dictionary mapping task_id to desired TaskState
        dag_run_id: Optional run_id for the DAG run

    Returns:
        Dictionary mapping task_id to the created TaskInstance

    Example:
        >>> from tests_common.test_utils.dag_testing import simulate_task_states, TaskState
        >>> with dag_maker("test_dag") as dag:
        ...     task1 = EmptyOperator(task_id="task1")
        ...     task2 = EmptyOperator(task_id="task2")
        ...     task1 >> task2
        >>> states = simulate_task_states(dag, {"task1": TaskState.SUCCESS, "task2": TaskState.RUNNING})
    """
    from uuid import uuid4

    task_instances = {}
    dag_version_id = uuid4()

    for task_id, state in task_states.items():
        task = dag.task_dict.get(task_id)
        if task is None:
            raise ValueError(f"Task '{task_id}' not found in DAG '{dag.dag_id}'")

        ti = TaskInstance(
            task=task,
            run_id=dag_run_id or "test_run_id",
            state=state.value if hasattr(state, "value") else state,
            dag_version_id=dag_version_id,
        )
        task_instances[task_id] = ti

    return task_instances


def simulate_dag_run(
    dag_maker: "DagMaker",
    task_states: dict[str, TaskState] | None = None,
    run_id: str | None = None,
) -> tuple[Any, dict[str, TaskInstance]]:
    """Simulate a DAG run with specific task states.

    This is a convenience function that creates a DAG run and optionally
    sets task instances to specific states.

    Args:
        dag_maker: The DagMaker fixture instance
        task_states: Optional dictionary mapping task_id to desired TaskState
        run_id: Optional run_id for the DAG run

    Returns:
        Tuple of (DagRun, dict of task_id to TaskInstance)

    Example:
        >>> with dag_maker("test_dag") as dag:
        ...     task1 = EmptyOperator(task_id="task1")
        ...     task2 = EmptyOperator(task_id="task2")
        ...     task1 >> task2
        >>> dr, tis = simulate_dag_run(dag_maker, {"task1": TaskState.SUCCESS})
    """
    dag_run = dag_maker.create_dagrun(run_id=run_id)
    task_instances = {}

    if task_states:
        task_instances = simulate_task_states(dag_maker.dag, task_states, run_id or dag_run.run_id)

    return dag_run, task_instances


# DAG structure assertion helpers


def assert_valid_dag(dag: "DAG") -> None:
    """Assert that a DAG is valid.

    Validates that the DAG has:
    - A valid dag_id
    - At least one task
    - No circular dependencies

    Args:
        dag: The DAG to validate

    Raises:
        AssertionError: If the DAG is invalid
    """
    assert dag.dag_id, "DAG must have a dag_id"
    assert dag.task_dict, "DAG must have at least one task"
    assert_no_cycles(dag)


def assert_task_order(
    dag: "DAG",
    expected_order: list[str],
) -> None:
    """Assert that tasks appear in the expected order in the DAG.

    This checks that each task in expected_order is downstream of
    the previous task (i.e., there's a path from each task to the next).

    Args:
        dag: The DAG to check
        expected_order: List of task_ids in expected execution order

    Raises:
        AssertionError: If the order doesn't match
    """
    if len(expected_order) < 2:
        return

    for i in range(len(expected_order) - 1):
        current_task_id = expected_order[i]
        next_task_id = expected_order[i + 1]

        current_task = dag.task_dict.get(current_task_id)
        next_task = dag.task_dict.get(next_task_id)

        assert current_task is not None, f"Task '{current_task_id}' not found in DAG"
        assert next_task is not None, f"Task '{next_task_id}' not found in DAG"

        # Check if there's a path from current to next
        downstream_ids = {t.task_id for t in current_task.get_flat_downstream()}
        assert next_task_id in downstream_ids, (
            f"Task '{next_task_id}' is not downstream of '{current_task_id}'. "
            f"Expected path: {current_task_id} -> {next_task_id}"
        )


def assert_no_cycles(dag: "DAG") -> None:
    """Assert that the DAG has no cycles.

    Args:
        dag: The DAG to check

    Raises:
        AssertionError: If the DAG contains a cycle
    """
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def visit(task_id: str) -> bool:
        visited.add(task_id)
        rec_stack.add(task_id)

        task = dag.task_dict.get(task_id)
        if task is None:
            return False

        for downstream in task.downstream_list:
            downstream_id = downstream.task_id
            if downstream_id not in visited:
                if visit(downstream_id):
                    return True
            elif downstream_id in rec_stack:
                # Build the cycle path for better error message
                cycle_path = _find_cycle_path(dag, downstream_id, task_id)
                raise AssertionError(
                    f"Cycle detected in DAG '{dag.dag_id}': {' -> '.join(cycle_path)}"
                )

        rec_stack.remove(task_id)
        return False

    for task_id in dag.task_dict:
        if task_id not in visited:
            if visit(task_id):
                raise AssertionError(f"Cycle detected in DAG '{dag.dag_id}'")


def _find_cycle_path(dag: "DAG", start_task_id: str, end_task_id: str) -> list[str]:
    """Find the cycle path between two tasks."""
    path = [end_task_id]

    def find_path(current_id: str, target_id: str) -> list[str] | None:
        if current_id == target_id:
            return [current_id]

        task = dag.task_dict.get(current_id)
        if task is None:
            return None

        for downstream in task.downstream_list:
            result = find_path(downstream.task_id, target_id)
            if result:
                return [current_id] + result

        return None

    result = find_path(start_task_id, end_task_id)
    if result:
        path = result + [start_task_id]
    return path


def assert_task_exists(dag: "DAG", task_id: str) -> None:
    """Assert that a task exists in the DAG.

    Args:
        dag: The DAG to check
        task_id: The task_id to look for

    Raises:
        AssertionError: If the task doesn't exist
    """
    assert task_id in dag.task_dict, f"Task '{task_id}' not found in DAG '{dag.dag_id}'"


def assert_task_depends_on(
    dag: "DAG",
    task_id: str,
    expected_upstream: list[str],
) -> None:
    """Assert that a task depends on the expected upstream tasks.

    Args:
        dag: The DAG to check
        task_id: The task_id to check
        expected_upstream: List of expected upstream task_ids

    Raises:
        AssertionError: If dependencies don't match
    """
    task = dag.task_dict.get(task_id)
    assert task is not None, f"Task '{task_id}' not found in DAG '{dag.dag_id}'"

    actual_upstream = {t.task_id for t in task.upstream_list}
    expected_set = set(expected_upstream)

    missing = expected_set - actual_upstream
    extra = actual_upstream - expected_set

    if missing or extra:
        msg = f"Task '{task_id}' upstream dependencies mismatch. "
        if missing:
            msg += f"Missing: {missing}. "
        if extra:
            msg += f"Unexpected: {extra}."
        raise AssertionError(msg)


def assert_no_orphans(dag: "DAG") -> None:
    """Assert that there are no orphan tasks in the DAG.

    An orphan task is one that is not connected to any other task
    (no upstream and no downstream).

    Args:
        dag: The DAG to check

    Raises:
        AssertionError: If there are orphan tasks
    """
    orphan_tasks = []

    for task_id, task in dag.task_dict.items():
        has_upstream = len(task.upstream_list) > 0
        has_downstream = len(task.downstream_list) > 0

        if not has_upstream and not has_downstream:
            orphan_tasks.append(task_id)

    assert not orphan_tasks, f"Found orphan tasks in DAG '{dag.dag_id}': {orphan_tasks}"


# Helper functions for DAG traversal


def get_task_by_id(dag: "DAG", task_id: str) -> "Operator | None":
    """Get a task by its task_id.

    Args:
        dag: The DAG to search
        task_id: The task_id to find

    Returns:
        The task if found, None otherwise
    """
    return dag.task_dict.get(task_id)


def get_all_tasks(dag: "DAG") -> list["Operator"]:
    """Get all tasks in the DAG.

    Args:
        dag: The DAG to get tasks from

    Returns:
        List of all tasks in the DAG
    """
    return list(dag.task_dict.values())


def get_direct_ancestors(task: "Operator") -> list["Operator"]:
    """Get direct ancestor tasks (upstream) of a task.

    Args:
        task: The task to get ancestors for

    Returns:
        List of direct upstream tasks
    """
    return list(task.upstream_list)


def get_direct_descendants(task: "Operator") -> list["Operator"]:
    """Get direct descendant tasks (downstream) of a task.

    Args:
        task: The task to get descendants for

    Returns:
        List of direct downstream tasks
    """
    return list(task.downstream_list)
