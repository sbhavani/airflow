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

"""Error diagnostics generator service."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow.utils.error_classifier import (
    classify_error,
    get_classifier,
)
from airflow.utils.error_diagnostics import (
    ErrorCategory,
    ErrorDiagnostics,
    generate_error_summary,
)

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


def generate_diagnostics(
    task_instance: TaskInstance,
    exception: BaseException | None = None,
) -> dict[str, Any] | None:
    """
    Generate error diagnostics for a failed task instance.

    Args:
        task_instance: The task instance that failed
        exception: The exception that caused the failure (optional)

    Returns:
        Dictionary containing error diagnostics, or None if no error
    """
    # Get the error information from the task instance
    error_message = _get_error_message(task_instance, exception)
    if not error_message:
        return None

    # Classify the error
    error_category, error_type = classify_error(error_message)

    # Generate human-readable summary
    error_summary = generate_error_summary(error_category, error_type, error_message)

    # Get remediation content
    from airflow.utils.remediation_loader import get_remediation_for_error

    remediation = get_remediation_for_error(error_category.value, error_type)

    # Build the diagnostics object
    diagnostics = ErrorDiagnostics(
        error_category=error_category.value,
        error_type=error_type,
        error_summary=error_summary,
        error_message=error_message,
        possible_causes=remediation.get("possible_causes", []) if remediation else [],
        remediation_steps=remediation.get("remediation_steps", []) if remediation else [],
        context=_extract_context(task_instance),
    )

    return _serialize_diagnostics(diagnostics)


def _get_error_message(task_instance: TaskInstance, exception: BaseException | None = None) -> str | None:
    """Extract error message from task instance or exception."""
    if exception:
        return str(exception)

    # Try to get error from task instance log or previous try
    if hasattr(task_instance, "error"):
        error = task_instance.error
        if error:
            return str(error)

    return None


def _extract_context(task_instance: TaskInstance) -> dict[str, Any]:
    """Extract relevant context from the task instance."""
    context = {
        "task_id": task_instance.task_id,
        "dag_id": task_instance.dag_id,
        "try_number": task_instance.try_number,
    }

    if hasattr(task_instance, "operator"):
        context["operator"] = task_instance.operator

    if hasattr(task_instance, "pool"):
        context["pool"] = task_instance.pool

    if hasattr(task_instance, "queue"):
        context["queue"] = task_instance.queue

    return context


def _serialize_diagnostics(diagnostics: ErrorDiagnostics) -> dict[str, Any]:
    """Serialize ErrorDiagnostics to a dictionary."""
    return {
        "error_category": diagnostics.error_category,
        "error_type": diagnostics.error_type,
        "error_summary": diagnostics.error_summary,
        "error_message": diagnostics.error_message,
        "possible_causes": diagnostics.possible_causes,
        "remediation_steps": diagnostics.remediation_steps,
        "context": diagnostics.context,
        "timestamp": diagnostics.timestamp,
    }
