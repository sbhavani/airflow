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
"""Error diagnostics module for providing actionable error information."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class ErrorDiagnostic:
    """A diagnostic suggestion for a task failure."""

    error_type: str
    possible_causes: Sequence[str]
    remediation_steps: Sequence[str]


# Error type to diagnostic mapping
# This maps common Airflow error types to their possible causes and remediation steps
ERROR_DIAGNOSTICS: dict[str, ErrorDiagnostic] = {
    "AirflowFailException": ErrorDiagnostic(
        error_type="AirflowFailException",
        possible_causes=[
            "Task raised a non-retryable failure",
            "DAG definition contains an error",
            "Task has fail_fast enabled and a dependent task failed",
            "Sensor determined condition was not met and should not retry",
        ],
        remediation_steps=[
            "Check the task logs for specific error details",
            "Review the DAG definition for syntax errors",
            "Verify the condition logic in sensors",
            "Consider adjusting the fail_fast setting if appropriate",
        ],
    ),
    "AirflowSensorTimeout": ErrorDiagnostic(
        error_type="AirflowSensorTimeout",
        possible_causes=[
            "Sensor timed out waiting for condition",
            "External system is unavailable or slow",
            "Sensor poke interval is too long",
            "Expected data or event has not arrived",
        ],
        remediation_steps=[
            "Increase the sensor timeout duration",
            "Reduce the poke interval for faster polling",
            "Check if the external system is operational",
            "Consider using a smarter sensor type (e.g., SmartSensor)",
            "Review the sensor's mode (poke vs reschedule)",
        ],
    ),
    "AirflowTaskTimeout": ErrorDiagnostic(
        error_type="AirflowTaskTimeout",
        possible_causes=[
            "Task execution exceeded the configured timeout",
            "Task is stuck in an infinite loop",
            "External service is not responding",
            "Task is waiting for a resource that is unavailable",
        ],
        remediation_steps=[
            "Increase the task execution timeout if needed",
            "Review the task code for infinite loops or blocking operations",
            "Check connectivity to external services",
            "Verify the task has access to required resources",
            "Consider breaking down the task into smaller units",
        ],
    ),
    "AirflowRuntimeError": ErrorDiagnostic(
        error_type="AirflowRuntimeError",
        possible_causes=[
            "Runtime error in task execution",
            "Invalid task configuration",
            "Missing required dependencies",
            "Error during XCom communication",
        ],
        remediation_steps=[
            "Check task logs for specific runtime error details",
            "Verify all required dependencies are installed",
            "Review task configuration and parameters",
            "Check XCom backend connectivity",
        ],
    ),
    "AirflowTaskTerminated": ErrorDiagnostic(
        error_type="AirflowTaskTerminated",
        possible_causes=[
            "Task was externally terminated (killed by user or system)",
            "Worker node was terminated or restarted",
            "Task exceeded memory limits",
            "Celery worker was shut down",
        ],
        remediation_steps=[
            "Check if the task was manually killed",
            "Review worker/node stability",
            "Increase task memory limits if applicable",
            "Check Celery worker logs for termination reasons",
            "Consider using retry logic for critical tasks",
        ],
    ),
    "AirflowException": ErrorDiagnostic(
        error_type="AirflowException",
        possible_causes=[
            "General Airflow exception during task execution",
            "Configuration error",
            "Hook failure",
            "Invalid operator usage",
        ],
        remediation_steps=[
            "Check task logs for specific error details",
            "Verify operator parameters are correct",
            "Review hook configuration and connectivity",
            "Check Airflow configuration for errors",
        ],
    ),
    "ConnectionError": ErrorDiagnostic(
        error_type="ConnectionError",
        possible_causes=[
            "Unable to connect to external service",
            "Network connectivity issues",
            "Authentication failure",
            "Service is unavailable",
        ],
        remediation_steps=[
            "Verify the connection details are correct",
            "Check network connectivity to the service",
            "Review credentials and authentication settings",
            "Check if the external service is running",
            "Verify firewall rules allow the connection",
        ],
    ),
    "TimeoutError": ErrorDiagnostic(
        error_type="TimeoutError",
        possible_causes=[
            "Operation took too long and timed out",
            "External service is slow or unresponsive",
            "Resource contention",
            "Query performance issues",
        ],
        remediation_steps=[
            "Increase timeout settings if appropriate",
            "Optimize the operation or query",
            "Check for resource contention",
            "Review external service performance",
            "Consider implementing retry logic",
        ],
    ),
    "PermissionError": ErrorDiagnostic(
        error_type="PermissionError",
        possible_causes=[
            "Insufficient permissions to access resource",
            "File or directory access denied",
            "Missing ACL configuration",
            "Running as wrong user",
        ],
        remediation_steps=[
            "Verify the task has required permissions",
            "Check file and directory permissions",
            "Review ACL and IAM settings",
            "Ensure task runs as correct user/service account",
        ],
    ),
    "FileNotFoundError": ErrorDiagnostic(
        error_type="FileNotFoundError",
        possible_causes=[
            "Required file or directory does not exist",
            "Incorrect file path",
            "File was deleted or moved",
            "Mount or volume not available",
        ],
        remediation_steps=[
            "Verify the file path is correct",
            "Check if file exists in expected location",
            "Review mount/volume configuration",
            "Ensure file is created before task runs",
        ],
    ),
    "KeyError": ErrorDiagnostic(
        error_type="KeyError",
        possible_causes=[
            "Missing key in dictionary or config",
            "Incorrect variable name",
            "XCom key not found",
            "Template variable not defined",
        ],
        remediation_steps=[
            "Check for typos in variable/key names",
            "Verify XCom push happened before pull",
            "Ensure template variables are defined",
            "Review task dependencies",
        ],
    ),
    "ValueError": ErrorDiagnostic(
        error_type="ValueError",
        possible_causes=[
            "Invalid value passed to function",
            "Incorrect parameter type or value",
            "Data validation failure",
            "Configuration error",
        ],
        remediation_steps=[
            "Check function parameters and types",
            "Validate input data",
            "Review configuration values",
            "Check for correct enum or constant values",
        ],
    ),
    "ImportError": ErrorDiagnostic(
        error_type="ImportError",
        possible_causes=[
            "Required Python package not installed",
            "Module not in Python path",
            "Circular import",
            "Missing provider package",
        ],
        remediation_steps=[
            "Install required Python package",
            "Verify provider is installed",
            "Check Python path configuration",
            "Review import statements for errors",
        ],
    ),
    "OperationalError": ErrorDiagnostic(
        error_type="OperationalError",
        possible_causes=[
            "Database operation failed",
            "Database connection lost",
            "Lock wait timeout exceeded",
            "Deadlock detected",
        ],
        remediation_steps=[
            "Check database connectivity",
            "Review database locks and transactions",
            "Increase timeout settings",
            "Check database logs for details",
            "Optimize query or operation",
        ],
    ),
    "AirflowRescheduleException": ErrorDiagnostic(
        error_type="AirflowRescheduleException",
        possible_causes=[
            "Task was rescheduled for later execution",
            "Resource was temporarily unavailable",
            "External dependency was not ready",
        ],
        remediation_steps=[
            "This is normal behavior for rescheduled tasks",
            "Check if the reschedule was expected",
            "Review task dependencies and timing",
        ],
    ),
    "AirflowSkipException": ErrorDiagnostic(
        error_type="AirflowSkipException",
        possible_causes=[
            "Task was intentionally skipped",
            "Branch operator determined skip path",
            "Upstream task condition not met",
        ],
        remediation_steps=[
            "This may be expected behavior based on task logic",
            "Review branch operator conditions",
            "Check upstream task outcomes",
        ],
    ),
    "TaskDeferred": ErrorDiagnostic(
        error_type="TaskDeferred",
        possible_causes=[
            "Task was deferred to trigger",
            "Waiting for trigger to fire",
            "Deferrable operator waiting for condition",
        ],
        remediation_steps=[
            "This is expected behavior for deferrable operators",
            "Check trigger status",
            "Review trigger configuration",
            "Ensure triggerer component is running",
        ],
    ),
}


def get_error_diagnostic(error_type: str) -> ErrorDiagnostic | None:
    """
    Get the error diagnostic for a given error type.

    Args:
        error_type: The type of error (e.g., "AirflowTaskTimeout")

    Returns:
        ErrorDiagnostic if found, None otherwise
    """
    return ERROR_DIAGNOSTICS.get(error_type)


def get_error_type_from_exception(exception: BaseException) -> str:
    """
    Extract the error type from an exception.

    Args:
        exception: The exception to analyze

    Returns:
        The error type name
    """
    error_type = type(exception).__name__

    # Check if the exception is a subclass of known Airflow exceptions
    try:
        from airflow.sdk.exceptions import (
            AirflowException,
            AirflowFailException,
            AirflowRuntimeError,
            AirflowSensorTimeout,
            AirflowTaskTerminated,
            AirflowTaskTimeout,
            AirflowRescheduleException,
            AirflowSkipException,
            TaskDeferred,
        )

        if isinstance(exception, AirflowFailException):
            return "AirflowFailException"
        elif isinstance(exception, AirflowSensorTimeout):
            return "AirflowSensorTimeout"
        elif isinstance(exception, AirflowTaskTimeout):
            return "AirflowTaskTimeout"
        elif isinstance(exception, AirflowRuntimeError):
            return "AirflowRuntimeError"
        elif isinstance(exception, AirflowTaskTerminated):
            return "AirflowTaskTerminated"
        elif isinstance(exception, AirflowRescheduleException):
            return "AirflowRescheduleException"
        elif isinstance(exception, AirflowSkipException):
            return "AirflowSkipException"
        elif isinstance(exception, TaskDeferred):
            return "TaskDeferred"
        elif isinstance(exception, AirflowException):
            return "AirflowException"
    except ImportError:
        pass

    return error_type


def get_error_diagnostic_from_exception(exception: BaseException | None) -> ErrorDiagnostic | None:
    """
    Get the error diagnostic for an exception.

    Args:
        exception: The exception to analyze

    Returns:
        ErrorDiagnostic if found, None otherwise
    """
    if exception is None:
        return None

    error_type = get_error_type_from_exception(exception)
    return get_error_diagnostic(error_type)
