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
"""DAG Structure Validation Utilities.

This module provides assertion functions for validating DAG structure.
"""

from __future__ import annotations

import re
from typing import Any

from airflow.models import DAG
from airflow.models.operator import Operator


def assert_task_exists(dag: DAG, task_id: str) -> None:
    """Assert that a task exists in the DAG.

    :param dag: The DAG to check
    :param task_id: The task_id to look for

    :raises AssertionError: If the task does not exist
    """
    task = dag.task_dict.get(task_id)
    assert task is not None, f"Task '{task_id}' does not exist in DAG '{dag.dag_id}'"


def assert_task_depends_on(dag: DAG, task_id: str, expected_upstream: list[str]) -> None:
    """Assert that a task has the expected upstream dependencies.

    :param dag: The DAG to check
    :param task_id: The task_id to check
    :param expected_upstream: List of expected upstream task_ids

    :raises AssertionError: If the dependencies don't match
    """
    assert_task_exists(dag, task_id)
    task = dag.task_dict[task_id]
    actual_upstream = [t.task_id for t in task.upstream_list]

    assert set(actual_upstream) == set(expected_upstream), (
        f"Task '{task_id}' upstream dependencies mismatch. "
        f"Expected: {sorted(expected_upstream)}, Got: {sorted(actual_upstream)}"
    )


def assert_task_has_downstream(dag: DAG, task_id: str, expected_downstream: list[str]) -> None:
    """Assert that a task has the expected downstream dependencies.

    :param dag: The DAG to check
    :param task_id: The task_id to check
    :param expected_downstream: List of expected downstream task_ids

    :raises AssertionError: If the dependencies don't match
    """
    assert_task_exists(dag, task_id)
    task = dag.task_dict[task_id]
    actual_downstream = [t.task_id for t in task.downstream_list]

    assert set(actual_downstream) == set(expected_downstream), (
        f"Task '{task_id}' downstream dependencies mismatch. "
        f"Expected: {sorted(expected_downstream)}, Got: {sorted(actual_downstream)}"
    )


def assert_no_circular_dependencies(dag: DAG) -> None:
    """Assert that the DAG has no circular dependencies.

    :param dag: The DAG to check

    :raises AssertionError: If circular dependencies are found
    """
    # Use DFS-based cycle detection
    task_dict = dag.task_dict

    if not task_dict:
        return  # Empty DAG has no cycles

    # Track visited nodes and recursion stack for cycle detection
    visited: set[str] = set()
    rec_stack: set[str] = set()
    path: list[str] = []

    def dfs(task_id: str) -> list[str] | None:
        """DFS to detect cycles. Returns the cycle path if found, None otherwise."""
        visited.add(task_id)
        rec_stack.add(task_id)
        path.append(task_id)

        task = task_dict.get(task_id)
        if task:
            for downstream in task.downstream_list:
                downstream_id = downstream.task_id
                if downstream_id not in visited:
                    result = dfs(downstream_id)
                    if result:
                        return result
                elif downstream_id in rec_stack:
                    # Found a cycle - extract the cycle portion
                    cycle_start = path.index(downstream_id)
                    return path[cycle_start:] + [downstream_id]

        path.pop()
        rec_stack.remove(task_id)
        return None

    # Start DFS from each unvisited task
    for task_id in task_dict:
        if task_id not in visited:
            cycle = dfs(task_id)
            if cycle:
                cycle_str = " -> ".join(cycle)
                raise AssertionError(f"Circular dependency detected: {cycle_str}")


def assert_task_naming_convention(dag: DAG, pattern: str) -> None:
    """Assert that all task names follow a naming convention.

    :param dag: The DAG to check
    :param pattern: Regular expression pattern for valid task names

    :raises AssertionError: If any task doesn't match the pattern
    """
    regex = re.compile(pattern)
    invalid_tasks = []

    for task_id in dag.task_dict:
        if not regex.match(task_id):
            invalid_tasks.append(task_id)

    assert not invalid_tasks, (
        f"The following tasks do not match naming convention '{pattern}': {invalid_tasks}"
    )


def get_dag_structure(dag: DAG) -> dict[str, Any]:
    """Get a representation of the DAG structure.

    :param dag: The DAG to analyze

    :returns: Dictionary containing task IDs and their dependencies
    """
    structure: dict[str, Any] = {
        "dag_id": dag.dag_id,
        "tasks": [],
        "dependencies": {},
    }

    for task in dag.tasks:
        structure["tasks"].append(task.task_id)
        structure["dependencies"][task.task_id] = {
            "upstream": [t.task_id for t in task.upstream_list],
            "downstream": [t.task_id for t in task.downstream_list],
        }

    return structure
