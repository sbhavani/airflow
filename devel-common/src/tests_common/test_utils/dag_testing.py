#
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
"""Testing utilities for DAGs: mock operators, task lifecycle simulation, and DAG structure validation."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.compat import DagSerialization, SerializedDAG
from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

try:
    from airflow.sdk import BaseOperator
except ImportError:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.sdk import DAG, Context
    from airflow.sdk.types import Operator as SdkOperator
    from airflow.serialization.definitions.dag import Operator as SerializedOperator
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.dagrun import DAGRun

__all__ = [
    "MockOperator",
    "create_mock_operator",
    "create_python_mock_operator",
    "create_sensor_mock_operator",
    "create_dag_instance",
    "create_task_instance",
    "simulate_task_run",
    "trigger_dag_run",
    "assert_dag_has_tasks",
    "assert_task_depends_on",
    "assert_no_cycles",
    "get_task_dependencies",
    "serialize_dag",
    "assert_serializable_dag",
]


# =============================================================================
# Mock Operator Classes
# =============================================================================


class MockOperator(BaseOperator):
    """Mock operator for testing purposes without executing real operators.

    This operator can be used in DAGs for testing without side effects.
    It supports configurable execute behavior via callable parameter.
    """

    template_fields: tuple[str, ...] = ("arg1", "arg2")

    def __init__(
        self,
        *,
        task_id: str,
        arg1: str = "",
        arg2: str = "",
        execute_callable: Callable[[Context], Any] | None = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.arg1 = arg1
        self.arg2 = arg2
        self._execute_callable = execute_callable

    def execute(self, context: Context) -> Any:
        """Execute the mock operator.

        If a custom callable was provided, it will be called with the context.
        Otherwise, returns None (no-op).
        """
        if self._execute_callable is not None:
            return self._execute_callable(context)
        return None


class MockSensorOperator(BaseOperator):
    """Mock sensor operator for testing sensor-like behavior.

    This operator simulates a sensor that returns a PokeReturnValue.
    """

    template_fields: tuple[str, ...] = ("poke_interval",)

    def __init__(
        self,
        *,
        task_id: str,
        poke_interval: float = 60.0,
        timeout: float = 60.0 * 60.0,
        poke_callable: Callable[[Context], bool] | None = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self._poke_callable = poke_callable

    def execute(self, context: Context) -> Any:
        """Execute the mock sensor.

        If a custom poke callable was provided, it will be called with the context.
        Otherwise, returns True immediately (sensor completes).
        """
        if self._poke_callable is not None:
            return self._poke_callable(context)
        return True


# =============================================================================
# Mock Operator Creation Functions
# =============================================================================


def create_mock_operator(
    name: str,
    *,
    execute_callable: Callable[[Context], Any] | None = None,
    **kwargs,
) -> type[MockOperator]:
    """Create a mock operator class with custom execute behavior.

    :param name: The task_id for the mock operator
    :param execute_callable: Optional callable to execute when the operator runs
    :param kwargs: Additional arguments passed to MockOperator

    Example:
        >>> operator = create_mock_operator("test_task", arg1="value")
        >>> dag = DAG("test_dag", taskflow=[operator()])
    """
    return MockOperator(task_id=name, execute_callable=execute_callable, **kwargs)


def create_python_mock_operator(
    name: str,
    python_callable: Callable[..., Any],
    **kwargs,
) -> type[MockOperator]:
    """Create a mock operator that behaves like PythonOperator.

    This allows testing PythonOperator-based DAGs without executing the actual callable.

    :param name: The task_id for the mock operator
    :param python_callable: The callable that would be executed (not called)
    :param kwargs: Additional arguments passed to MockOperator

    Example:
        >>> def my_func():
        ...     return "result"
        >>> operator = create_python_mock_operator("python_task", my_func)
        >>> dag = DAG("test_dag", taskflow=[operator()])
    """
    return MockOperator(task_id=name, execute_callable=python_callable, **kwargs)


def create_sensor_mock_operator(
    name: str,
    *,
    poke_callable: Callable[[Context], bool] | None = None,
    poke_interval: float = 60.0,
    timeout: float = 60.0 * 60.0,
    **kwargs,
) -> type[MockSensorOperator]:
    """Create a mock sensor operator for testing sensor-like behavior.

    :param name: The task_id for the sensor operator
    :param poke_callable: Optional callable that returns True when sensor should proceed
    :param poke_interval: Interval between pokes in seconds
    :param timeout: Total timeout in seconds
    :param kwargs: Additional arguments passed to MockSensorOperator

    Example:
        >>> sensor = create_sensor_mock_operator("wait_for_file")
        >>> dag = DAG("test_dag", taskflow=[sensor()])
    """
    return MockSensorOperator(
        task_id=name,
        poke_callable=poke_callable,
        poke_interval=poke_interval,
        timeout=timeout,
        **kwargs,
    )


# =============================================================================
# Task Instance Creation Helpers
# =============================================================================


def create_dag_instance(
    dag_id: str,
    *,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    schedule_interval: str | None = None,
    **kwargs,
) -> DAG:
    """Create a DAG instance for testing.

    :param dag_id: The DAG ID
    :param start_date: Optional start date for the DAG
    :param end_date: Optional end date for the DAG
    :param schedule_interval: Optional schedule interval
    :param kwargs: Additional arguments passed to DAG constructor

    Example:
        >>> dag = create_dag_instance("test_dag", schedule_interval="@daily")
    """
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import DAG

        return DAG(
            dag_id=dag_id,
            start_date=start_date,
            end_date=end_date,
            schedule_interval=schedule_interval,
            **kwargs,
        )
    else:
        from airflow import DAG

        return DAG(
            dag_id=dag_id,
            start_date=start_date,
            end_date=end_date,
            schedule_interval=schedule_interval,
            **kwargs,
        )


def create_task_instance(
    task: SdkOperator | SerializedOperator,
    dag: DAG | SerializedDAG,
    *,
    run_id: str | None = None,
    state: TaskInstanceState | None = None,
    execution_date: datetime | None = None,
    map_index: int = -1,
    session: Session = NEW_SESSION,
    **kwargs,
) -> TaskInstance:
    """Create a TaskInstance with configurable state for testing.

    :param task: The task (operator) to create instance for
    :param dag: The DAG the task belongs to
    :param run_id: Optional run ID
    :param state: Optional initial state
    :param execution_date: Optional execution date
    :param map_index: Map index for mapped tasks
    :param session: Database session
    :param kwargs: Additional arguments passed to TaskInstance

    Example:
        >>> dag = create_dag_instance("test_dag")
        >>> task = EmptyOperator(task_id="task1", dag=dag)
        >>> ti = create_task_instance(task, dag, state=TaskInstanceState.SUCCESS)

    Note: This is a wrapper around the existing create_task_instance from
    tests_common.test_utils.taskinstance with additional convenience parameters.
    """
    from tests_common.test_utils.taskinstance import create_task_instance as _create_task_instance

    # Get dag_version_id for Airflow 3.x
    if AIRFLOW_V_3_0_PLUS:
        # For SerializedDAG, we need to get the dag_version_id
        if isinstance(dag, SerializedDAG):
            dag_version_id = dag.dag_version_id
        else:
            # For SDK DAG, we need to serialize it first
            serialized = create_scheduler_dag(dag)
            dag_version_id = serialized.dag_version_id
    else:
        dag_version_id = None  # type: ignore[assignment]

    return _create_task_instance(
        task,
        dag_version_id=dag_version_id,
        run_id=run_id,
        state=state,
        map_index=map_index,
        **kwargs,
    )


# =============================================================================
# Task Lifecycle Simulation
# =============================================================================


def simulate_task_run(
    task_instance: TaskInstance,
    context: Context | None = None,
    *,
    ignore_depends_on_past: bool = False,
    ignore_task_deps: bool = False,
    ignore_ti_state: bool = False,
    mark_success: bool = False,
    mock_result: Any = None,
    mock_exception: Exception | None = None,
    session: Session = NEW_SESSION,
) -> TaskInstance:
    """Simulate a task run through its lifecycle states.

    This function wraps the existing run_task_instance and allows injecting
    mock results or exceptions at each stage.

    :param task_instance: The task instance to run
    :param context: Optional execution context
    :param ignore_depends_on_past: Ignore depends_on_past flag
    :param ignore_task_deps: Ignore task dependencies
    :param ignore_ti_state: Ignore current task instance state
    :param mark_success: Mark task as successful without running
    :param mock_result: Optional mock result to return from execution
    :param mock_exception: Optional exception to raise during execution
    :param session: Database session

    Example:
        >>> dag = create_dag_instance("test_dag")
        >>> task = EmptyOperator(task_id="task1", dag=dag)
        >>> ti = create_task_instance(task, dag, state=TaskInstanceState.SCHEDULED)
        >>> result = simulate_task_run(ti, mock_result="success")
    """
    from tests_common.test_utils.taskinstance import run_task_instance

    # If mock result or exception is provided, we need to patch the execute method
    original_execute = None
    task = task_instance.task

    if mock_result is not None or mock_exception is not None:
        original_execute = task.execute

        def mock_execute(ctx: Context) -> Any:
            if mock_exception is not None:
                raise mock_exception
            return mock_result

        task.execute = mock_execute

    try:
        result = run_task_instance(
            task_instance,
            task,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            session=session,
        )
        return result
    finally:
        # Restore original execute method
        if original_execute is not None:
            task.execute = original_execute


@provide_session
def trigger_dag_run(
    dag: DAG | SerializedDAG,
    *,
    run_id: str | None = None,
    execution_date: datetime | None = None,
    state: str | None = None,
    session: Session = NEW_SESSION,
    **kwargs,
) -> DAGRun:
    """Trigger a DAG run for testing.

    :param dag: The DAG to run
    :param run_id: Optional run ID (generated if not provided)
    :param execution_date: Optional execution date
    :param state: Optional initial state for the DAG run
    :param session: Database session
    :param kwargs: Additional arguments passed to DAGRun

    Example:
        >>> dag = create_dag_instance("test_dag")
        >>> dag_run = trigger_dag_run(dag, state="running")
    """
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import timezone

        now = timezone.utcnow()
    else:
        from airflow.utils import timezone

        now = timezone.utcnow()

    if run_id is None:
        run_id = f"test_run_{now.isoformat()}"

    if execution_date is None:
        execution_date = now

    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dagrun import DAGRun

        dag_run = DAGRun(
            dag_id=dag.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            state=state,
            **kwargs,
        )
    else:
        from airflow.models.dagrun import DAGRun

        dag_run = DAGRun(
            dag_id=dag.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            state=state,
            **kwargs,
        )

    session.add(dag_run)
    session.flush()
    return dag_run


# =============================================================================
# DAG Structure Assertion Helpers
# =============================================================================


def assert_dag_has_tasks(dag: DAG | SerializedDAG, expected_tasks: list[str]) -> None:
    """Assert that a DAG contains all expected tasks.

    :param dag: The DAG to validate
    :param expected_tasks: List of expected task IDs

    Raises:
        AssertionError: If any expected task is missing

    Example:
        >>> dag = load_dag("my_dag")
        >>> assert_dag_has_tasks(dag, ["task1", "task2", "task3"])
    """
    if isinstance(dag, SerializedDAG):
        actual_tasks = set(dag.task_dict.keys())
    else:
        actual_tasks = set(task.task_id for task in dag.taskflow)

    expected_set = set(expected_tasks)
    missing = expected_set - actual_tasks

    if missing:
        raise AssertionError(f"DAG is missing expected tasks: {missing}")


def assert_task_depends_on(
    dag: DAG | SerializedDAG,
    task_id: str,
    expected_upstream_ids: list[str],
) -> None:
    """Assert that a task has the expected upstream dependencies.

    :param dag: The DAG to validate
    :param task_id: The task ID to check
    :param expected_upstream_ids: List of expected upstream task IDs

    Raises:
        AssertionError: If upstream dependencies don't match

    Example:
        >>> dag = load_dag("my_dag")
        >>> assert_task_depends_on(dag, "task2", ["task1"])
    """
    if isinstance(dag, SerializedDAG):
        task = dag.task_dict.get(task_id)
        if task is None:
            raise AssertionError(f"Task '{task_id}' not found in DAG")
        upstream_task_ids = [t.task_id for t in task.upstream_list]
    else:
        # For SDK DAG, find the task in taskflow
        task = None
        for t in dag.taskflow:
            if t.task_id == task_id:
                task = t
                break
        if task is None:
            raise AssertionError(f"Task '{task_id}' not found in DAG")
        upstream_task_ids = [t.task_id for t in task.upstream_list]

    expected_set = set(expected_upstream_ids)
    actual_set = set(upstream_task_ids)

    if expected_set != actual_set:
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        msg = f"Task '{task_id}' upstream dependencies don't match"
        if missing:
            msg += f". Missing: {missing}"
        if extra:
            msg += f". Extra: {extra}"
        raise AssertionError(msg)


def assert_no_cycles(dag: DAG | SerializedDAG) -> None:
    """Assert that a DAG has no cycles (is a valid DAG).

    :param dag: The DAG to validate

    Raises:
        AssertionError: If the DAG contains cycles

    Example:
        >>> dag = load_dag("my_dag")
        >>> assert_no_cycles(dag)
    """
    # Use topological sort to detect cycles
    if isinstance(dag, SerializedDAG):
        tasks = dag.task_dict.values()
    else:
        tasks = dag.taskflow

    # Build adjacency list
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def has_cycle(task_id: str) -> bool:
        visited.add(task_id)
        rec_stack.add(task_id)

        # Get upstream tasks
        if isinstance(dag, SerializedDAG):
            task = dag.task_dict.get(task_id)
            upstream = [t.task_id for t in task.upstream_list] if task else []
        else:
            for t in dag.taskflow:
                if t.task_id == task_id:
                    upstream = [u.task_id for u in t.upstream_list]
                    break
            else:
                upstream = []

        for upstream_id in upstream:
            if upstream_id not in visited:
                if has_cycle(upstream_id):
                    return True
            elif upstream_id in rec_stack:
                return True

        rec_stack.remove(task_id)
        return False

    # Check each task
    for task in tasks:
        task_id = task.task_id
        if task_id not in visited:
            if has_cycle(task_id):
                raise AssertionError(f"DAG contains a cycle involving task '{task_id}'")


def get_task_dependencies(dag: DAG | SerializedDAG, task_id: str) -> dict[str, list[str]]:
    """Get upstream and downstream dependencies for a task.

    :param dag: The DAG to analyze
    :param task_id: The task ID to get dependencies for

    Returns:
        Dictionary with 'upstream' and 'downstream' lists of task IDs

    Example:
        >>> dag = load_dag("my_dag")
        >>> deps = get_task_dependencies(dag, "task2")
        >>> print(deps)  # {'upstream': ['task1'], 'downstream': ['task3']}
    """
    if isinstance(dag, SerializedDAG):
        task = dag.task_dict.get(task_id)
        if task is None:
            raise ValueError(f"Task '{task_id}' not found in DAG")
        upstream = [t.task_id for t in task.upstream_list]
        downstream = [t.task_id for t in task.downstream_list]
    else:
        # For SDK DAG
        target_task = None
        for t in dag.taskflow:
            if t.task_id == task_id:
                target_task = t
                break
        if target_task is None:
            raise ValueError(f"Task '{task_id}' not found in DAG")
        upstream = [t.task_id for t in target_task.upstream_list]
        downstream = [t.task_id for t in target_task.downstream_list]

    return {"upstream": upstream, "downstream": downstream}


# =============================================================================
# DAG Serialization Testing
# =============================================================================


def serialize_dag(dag: DAG) -> SerializedDAG:
    """Serialize a DAG for the scheduler.

    :param dag: The DAG to serialize

    Returns:
        SerializedDAG object

    Example:
        >>> dag = create_dag_instance("test_dag")
        >>> # Add tasks to dag...
        >>> serialized = serialize_dag(dag)
    """
    return DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))


def assert_serializable_dag(dag: DAG) -> SerializedDAG:
    """Assert that a DAG can be serialized for the scheduler.

    :param dag: The DAG to validate

    Returns:
        SerializedDAG object if serialization succeeds

    Raises:
        AssertionError: If serialization fails

    Example:
        >>> dag = create_dag_instance("test_dag")
        >>> # Add tasks to dag...
        >>> serialized = assert_serializable_dag(dag)
    """
    try:
        serialized = serialize_dag(dag)
        return serialized
    except Exception as e:
        raise AssertionError(f"DAG '{dag.dag_id}' failed to serialize: {e}")


