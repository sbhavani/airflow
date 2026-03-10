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

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING

from airflow.utils.session import NEW_SESSION, provide_session

from tests_common.test_utils.compat import DagSerialization, SerializedDAG

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk import DAG


def create_scheduler_dag(dag: DAG | SerializedDAG) -> SerializedDAG:
    if isinstance(dag, SerializedDAG):
        return dag
    return DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))


@provide_session
def sync_dag_to_db(
    dag: DAG,
    bundle_name: str = "testing",
    session: Session = NEW_SESSION,
) -> SerializedDAG:
    return sync_dags_to_db([dag], bundle_name=bundle_name, session=session)[0]


@provide_session
def sync_dags_to_db(
    dags: Collection[DAG],
    bundle_name: str = "testing",
    session: Session = NEW_SESSION,
) -> Sequence[SerializedDAG]:
    """
    Sync dags into the database.

    This serializes dags and saves the results to the database. The serialized
    (scheduler-oeirnted) dags are returned. If the input is ordered (e.g. a list),
    the returned sequence is guaranteed to be in the same order.
    """
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

    session.merge(DagBundleModel(name=bundle_name))
    session.flush()

    def _write_dag(dag: DAG) -> SerializedDAG:
        data = DagSerialization.to_dict(dag)
        SerializedDagModel.write_dag(LazyDeserializedDAG(data=data), bundle_name, session=session)
        return DagSerialization.from_dict(data)

    SerializedDAG.bulk_write_to_db(bundle_name, None, dags, session=session)
    scheduler_dags = [_write_dag(dag) for dag in dags]
    session.flush()
    return scheduler_dags


# =============================================================================
# Task Lifecycle Simulation Helpers
# =============================================================================


def get_dag_tasks(dag: DAG | SerializedDAG) -> dict[str, "Operator"]:
    """
    Get all tasks in a DAG as a dictionary keyed by task_id.

    :param dag: The DAG to get tasks from.
    :return: Dictionary mapping task_id to task.
    """
    return dag.task_dict


def get_dag_task_ids(dag: DAG | SerializedDAG) -> set[str]:
    """
    Get all task IDs in a DAG.

    :param dag: The DAG to get task IDs from.
    :return: Set of task IDs.
    """
    return set(dag.task_dict.keys())


def get_task_upstream_ids(dag: DAG | SerializedDAG, task_id: str) -> set[str]:
    """
    Get the IDs of all upstream tasks for a given task.

    :param dag: The DAG containing the task.
    :param task_id: The ID of the task.
    :return: Set of upstream task IDs.
    """
    task = dag.get_task(task_id)
    return task.upstream_task_ids


def get_task_downstream_ids(dag: DAG | SerializedDAG, task_id: str) -> set[str]:
    """
    Get the IDs of all downstream tasks for a given task.

    :param dag: The DAG containing the task.
    :param task_id: The ID of the task.
    :return: Set of downstream task IDs.
    """
    task = dag.get_task(task_id)
    return task.downstream_task_ids


def get_all_upstream_ids(dag: DAG | SerializedDAG, task_id: str) -> set[str]:
    """
    Get all upstream task IDs (transitively) for a given task.

    :param dag: The DAG containing the task.
    :param task_id: The ID of the task.
    :return: Set of all upstream task IDs.
    """
    task = dag.get_task(task_id)
    return task.get_flat_relative_ids(upstream=True)


def get_all_downstream_ids(dag: DAG | SerializedDAG, task_id: str) -> set[str]:
    """
    Get all downstream task IDs (transitively) for a given task.

    :param dag: The DAG containing the task.
    :param task_id: The ID of the task.
    :return: Set of all downstream task IDs.
    """
    task = dag.get_task(task_id)
    return task.get_flat_relative_ids(upstream=False)


def get_dag_roots(dag: DAG | SerializedDAG) -> list[str]:
    """
    Get the root tasks (tasks with no upstream dependencies) in a DAG.

    :param dag: The DAG to get root tasks from.
    :return: List of root task IDs.
    """
    return [task_id for task_id, task in dag.task_dict.items() if not task.upstream_task_ids]


def get_dag_leaves(dag: DAG | SerializedDAG) -> list[str]:
    """
    Get the leaf tasks (tasks with no downstream dependencies) in a DAG.

    :param dag: The DAG to get leaf tasks from.
    :return: List of leaf task IDs.
    """
    return [task_id for task_id, task in dag.task_dict.items() if not task.downstream_task_ids]


def get_task_depth(dag: DAG | SerializedDAG, task_id: str) -> int:
    """
    Get the depth of a task in the DAG (distance from root).

    :param dag: The DAG containing the task.
    :param task_id: The ID of the task.
    :return: The depth of the task (0 for root tasks).
    """
    task = dag.get_task(task_id)
    if not task.upstream_task_ids:
        return 0

    max_depth = 0
    for upstream_id in task.upstream_task_ids:
        depth = get_task_depth(dag, upstream_id)
        max_depth = max(max_depth, depth)
    return max_depth + 1


# =============================================================================
# DAG Structure Assertion Helpers
# =============================================================================


def assert_dag_has_tasks(dag: DAG | SerializedDAG, expected_task_ids: list[str] | set[str]) -> None:
    """
    Assert that a DAG contains all expected tasks.

    :param dag: The DAG to check.
    :param expected_task_ids: List or set of expected task IDs.
    :raises AssertionError: If any expected task is not found.
    """
    expected = set(expected_task_ids)
    actual = set(dag.task_dict.keys())
    missing = expected - actual
    if missing:
        raise AssertionError(f"DAG is missing expected tasks: {missing}")


def assert_task_in_dag(dag: DAG | SerializedDAG, task_id: str) -> None:
    """
    Assert that a specific task exists in the DAG.

    :param dag: The DAG to check.
    :param task_id: The task ID to look for.
    :raises AssertionError: If the task is not found.
    """
    if task_id not in dag.task_dict:
        raise AssertionError(f"Task '{task_id}' not found in DAG '{dag.dag_id}'")


def assert_task_not_in_dag(dag: DAG | SerializedDAG, task_id: str) -> None:
    """
    Assert that a specific task does not exist in the DAG.

    :param dag: The DAG to check.
    :param task_id: The task ID that should not exist.
    :raises AssertionError: If the task exists.
    """
    if task_id in dag.task_dict:
        raise AssertionError(f"Task '{task_id}' should not exist in DAG '{dag.dag_id}'")


def assert_dag_dependencies(
    dag: DAG | SerializedDAG,
    task_id: str,
    upstream: list[str] | set[str] | None = None,
    downstream: list[str] | set[str] | None = None,
) -> None:
    """
    Assert that a task has the expected upstream and/or downstream dependencies.

    :param dag: The DAG to check.
    :param task_id: The task ID to verify.
    :param upstream: Expected upstream task IDs.
    :param downstream: Expected downstream task IDs.
    :raises AssertionError: If dependencies don't match.
    """
    task = dag.get_task(task_id)

    if upstream is not None:
        expected_upstream = set(upstream)
        actual_upstream = task.upstream_task_ids
        missing_upstream = expected_upstream - actual_upstream
        extra_upstream = actual_upstream - expected_upstream
        if missing_upstream or extra_upstream:
            raise AssertionError(
                f"Task '{task_id}' upstream dependencies mismatch. "
                f"Missing: {missing_upstream}, Extra: {extra_upstream}"
            )

    if downstream is not None:
        expected_downstream = set(downstream)
        actual_downstream = task.downstream_task_ids
        missing_downstream = expected_downstream - actual_downstream
        extra_downstream = actual_downstream - expected_downstream
        if missing_downstream or extra_downstream:
            raise AssertionError(
                f"Task '{task_id}' downstream dependencies mismatch. "
                f"Missing: {missing_downstream}, Extra: {extra_downstream}"
            )


def assert_dag_has_single_root(dag: DAG | SerializedDAG) -> None:
    """
    Assert that a DAG has exactly one root task.

    :param dag: The DAG to check.
    :raises AssertionError: If the DAG has zero or more than one root.
    """
    roots = get_dag_roots(dag)
    if len(roots) != 1:
        raise AssertionError(f"DAG '{dag.dag_id}' should have exactly one root, found: {roots}")


def assert_dag_has_no_cycles(dag: DAG | SerializedDAG) -> None:
    """
    Assert that a DAG has no cycles.

    :param dag: The DAG to check.
    :raises AssertionError: If a cycle is detected.
    """
    try:
        dag.check_cycle()
    except Exception as e:
        raise AssertionError(f"DAG '{dag.dag_id}' has a cycle: {e}")


def assert_dag_is_connected(dag: DAG | SerializedDAG) -> None:
    """
    Assert that all tasks in the DAG are connected (no orphaned tasks).

    :param dag: The DAG to check.
    :raises AssertionError: If there are disconnected tasks.
    """
    if not dag.task_dict:
        return

    # Start from any root and traverse all reachable tasks
    roots = get_dag_roots(dag)
    if not roots:
        # No roots means all tasks have dependencies - check if all tasks are reachable from any task
        any_task_id = next(iter(dag.task_dict))
        reachable = get_all_upstream_ids(dag, any_task_id) | {any_task_id}
    else:
        reachable: set[str] = set()
        for root_id in roots:
            reachable.add(root_id)
            reachable.update(get_all_downstream_ids(dag, root_id))

    all_tasks = set(dag.task_dict.keys())
    disconnected = all_tasks - reachable

    if disconnected:
        raise AssertionError(
            f"DAG '{dag.dag_id}' has disconnected tasks: {disconnected}"
        )


def assert_task_upstream_of(
    dag: DAG | SerializedDAG,
    upstream_task_id: str,
    downstream_task_id: str,
) -> None:
    """
    Assert that one task is upstream of another.

    :param dag: The DAG to check.
    :param upstream_task_id: The task that should be upstream.
    :param downstream_task_id: The task that should be downstream.
    :raises AssertionError: If the upstream relationship doesn't exist.
    """
    downstream_task = dag.get_task(downstream_task_id)
    if upstream_task_id not in downstream_task.upstream_task_ids:
        raise AssertionError(
            f"Task '{upstream_task_id}' is not upstream of task '{downstream_task_id}'"
        )


def assert_task_downstream_of(
    dag: DAG | SerializedDAG,
    downstream_task_id: str,
    upstream_task_id: str,
) -> None:
    """
    Assert that one task is downstream of another.

    :param dag: The DAG to check.
    :param downstream_task_id: The task that should be downstream.
    :param upstream_task_id: The task that should be upstream.
    :raises AssertionError: If the downstream relationship doesn't exist.
    """
    assert_task_upstream_of(dag, upstream_task_id, downstream_task_id)


def get_dag_structure_dict(dag: DAG | SerializedDAG) -> dict[str, list[str]]:
    """
    Get a dictionary representation of the DAG structure.

    :param dag: The DAG to get structure from.
    :return: Dictionary mapping task_id to list of downstream task IDs.
    """
    return {
        task_id: sorted(list(task.downstream_task_ids))
        for task_id, task in dag.task_dict.items()
    }


def get_dag_level_order(dag: DAG | SerializedDAG) -> list[list[str]]:
    """
    Get tasks in the DAG organized by level (breadth-first).

    :param dag: The DAG to get level order from.
    :return: List of lists, where each inner list contains task IDs at that depth.
    """
    if not dag.task_dict:
        return []

    roots = get_dag_roots(dag)
    if not roots:
        # Find tasks that are upstream of others
        roots = get_dag_roots(dag)
        if not roots:
            return []

    levels: list[list[str]] = []
    visited: set[str] = set()
    current_level = roots

    while current_level:
        levels.append(current_level)
        visited.update(current_level)

        next_level: list[str] = []
        for task_id in current_level:
            task = dag.get_task(task_id)
            for downstream_id in task.downstream_task_ids:
                if downstream_id not in visited:
                    next_level.append(downstream_id)

        current_level = next_level

    return levels


# Type alias for the Operator
if TYPE_CHECKING:
    from airflow.sdk.types import Operator
