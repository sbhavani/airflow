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
"""DAG Serialization Validation Utilities.

This module provides helpers to validate that DAGs can be serialized correctly.
"""

from __future__ import annotations

import json
from typing import Any

from airflow.models import DAG


def validate_dag_serialization(dag: DAG) -> list[str]:
    """Validate that a DAG can be serialized correctly.

    This is important because Airflow's scheduler needs to serialize
    DAGs to store them in the database.

    :param dag: The DAG to validate

    :returns: List of error messages (empty if validation passes)
    """
    errors: list[str] = []

    try:
        # Try to serialize the DAG
        serialized = dag.get_serialized()
        json.dumps(serialized)
    except Exception as e:
        errors.append(f"Serialization failed: {str(e)}")

    # Check for tasks without task_id
    for task in dag.tasks:
        if not task.task_id:
            errors.append(f"Task missing task_id: {task}")

    # Check for duplicate task_ids
    task_ids = [task.task_id for task in dag.tasks]
    if len(task_ids) != len(set(task_ids)):
        duplicates = set([tid for tid in task_ids if task_ids.count(tid) > 1])
        errors.append(f"Duplicate task_ids found: {duplicates}")

    # Check that all tasks belong to this DAG
    for task in dag.tasks:
        if task.dag is not dag:
            errors.append(
                f"Task '{task.task_id}' belongs to a different DAG "
                f"(expected: {dag.dag_id}, got: {task.dag.dag_id if task.dag else None})"
            )

    return errors


def get_serialization_errors(dag: DAG) -> dict[str, Any]:
    """Get detailed serialization errors for a DAG.

    This function provides more detailed error information than
    validate_dag_serialization, including which specific components
    failed to serialize.

    :param dag: The DAG to check

    :returns: Dictionary with error details
    """
    result: dict[str, Any] = {
        "dag_id": dag.dag_id,
        "valid": True,
        "errors": [],
        "task_errors": {},
    }

    # Check DAG-level serialization
    try:
        serialized = dag.get_serialized()
        json.dumps(serialized)
    except Exception as e:
        result["valid"] = False
        result["errors"].append(f"DAG serialization failed: {str(e)}")

    # Check each task
    for task in dag.tasks:
        task_errors: list[str] = []

        if not task.task_id:
            task_errors.append("Task missing task_id")

        # Try to serialize the task
        try:
            # Check if task has required attributes
            if not hasattr(task, "execute"):
                task_errors.append("Task missing execute method")

            # Check for unserializable attributes
            if hasattr(task, "operator_extra_link_class"):
                # Check if extra links are serializable
                try:
                    json.dumps(task.operator_extra_link_class)
                except (TypeError, ValueError):
                    task_errors.append("operator_extra_link_class is not JSON serializable")

        except Exception as e:
            task_errors.append(f"Task validation failed: {str(e)}")

        if task_errors:
            task_id = task.task_id or f"<unnamed_task_{id(task)}>"
            result["task_errors"][task_id] = task_errors
            result["valid"] = False

    if not result["valid"]:
        result["errors"].append(
            f"{len(result['task_errors'])} task(s) have serialization errors"
        )

    return result
