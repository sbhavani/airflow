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
"""Error diagnostics module for TaskInstance error analysis and suggestions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, Field


class ErrorDiagnostic(BaseModel):
    """A diagnostic suggestion for a task failure."""

    title: str = Field(description="Brief title describing the issue")
    description: str = Field(description="Detailed explanation of the issue")
    action: str = Field(description="Recommended remediation steps")
    documentation_url: str | None = Field(
        default=None, description="Optional link to documentation"
    )


class ErrorDiagnosticsResult(BaseModel):
    """Result of error diagnostics analysis."""

    error_category: str = Field(description="Category of the error")
    error_summary: str = Field(description="Short summary of the error")
    diagnostics: Annotated[list[ErrorDiagnostic], Field(default_factory=list)] = (
        Field(description="List of diagnostic suggestions")
    )


# Error category constants
class ErrorCategory:
    """Error categories for classifying task failures."""

    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    RESOURCE = "resource"
    TIMEOUT = "timeout"
    VALIDATION = "validation"
    EXTERNAL_SERVICE = "external_service"
    CODE_ERROR = "code_error"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


# Common error patterns and their diagnostics
ERROR_PATTERNS: list[tuple[re.Pattern[str], str, list[ErrorDiagnostic]]] = [
    # Connection errors
    (
        re.compile(
            r"(connection.*refused|connection.*timeout|ECONNREFUSED|ConnectionError|failed to connect)",
            re.IGNORECASE,
        ),
        ErrorCategory.CONNECTION,
        [
            ErrorDiagnostic(
                title="Connection Refused",
                description="The task failed to connect to a remote service. This could be due to the service being down, network issues, or incorrect connection parameters.",
                action="1. Verify the remote service is running and accessible\n2. Check network connectivity and firewall rules\n3. Verify the connection string/hostname is correct\n4. Ensure the required port is open",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html",
            )
        ],
    ),
    # Authentication errors
    (
        re.compile(
            r"(authentication.*fail|auth.*error|unauthorized|401|403|invalid.*credentials|login.*fail)",
            re.IGNORECASE,
        ),
        ErrorCategory.AUTHENTICATION,
        [
            ErrorDiagnostic(
                title="Authentication Failed",
                description="The task failed to authenticate with a remote service. This usually indicates invalid credentials or missing authentication tokens.",
                action="1. Verify the credentials are correct\n2. Check if the credentials have expired\n3. Ensure the user has necessary permissions\n4. Check for any required API keys or tokens",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/connections.html",
            )
        ],
    ),
    # Permission errors
    (
        re.compile(
            r"(permission.*denied|access.*denied|not authorized|unauthorized.*access|403 Forbidden)",
            re.IGNORECASE,
        ),
        ErrorCategory.PERMISSION,
        [
            ErrorDiagnostic(
                title="Permission Denied",
                description="The task does not have the required permissions to perform the operation.",
                action="1. Check the service account/role has necessary permissions\n2. Verify the IAM policies are correctly configured\n3. Ensure the user has access to the specific resource\n4. Review the resource ACLs",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/security/index.html",
            )
        ],
    ),
    # Resource errors (out of memory, disk space, etc.)
    (
        re.compile(
            r"(out of memory|oom|memory.*error|disk.*full|no space left|resource.*exhausted)",
            re.IGNORECASE,
        ),
        ErrorCategory.RESOURCE,
        [
            ErrorDiagnostic(
                title="Resource Exhausted",
                description="The task ran out of system resources such as memory or disk space.",
                action="1. Increase the pool slots allocated to the task\n2. Optimize the task to use less memory\n3. Check for memory leaks in the code\n4. Consider using a machine with more resources",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/pools.html",
            )
        ],
    ),
    # Timeout errors
    (
        re.compile(
            r"(timeout|timed out|deadline.*exceeded|ReadTimeout|ConnectTimeout)",
            re.IGNORECASE,
        ),
        ErrorCategory.TIMEOUT,
        [
            ErrorDiagnostic(
                title="Operation Timeout",
                description="The task operation took longer than the allowed time limit.",
                action="1. Increase the timeout value in the operator\n2. Check if the remote service is experiencing slow response times\n3. Optimize the operation to complete faster\n4. Check for network latency issues",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#timeouts",
            )
        ],
    ),
    # Validation errors
    (
        re.compile(
            r"(validation.*error|invalid.*input|schema.*error|malformed|ValueError|TypeError)",
            re.IGNORECECASE,
        ),
        ErrorCategory.VALIDATION,
        [
            ErrorDiagnostic(
                title="Validation Error",
                description="The task received invalid data that failed validation checks.",
                action="1. Check the input data format and structure\n2. Verify all required fields are present\n3. Check for type mismatches in the data\n4. Review recent changes to upstream tasks",
                documentation_url=None,
            )
        ],
    ),
    # External service errors
    (
        re.compile(
            r"(api.*error|http.*error|5\d\d|service.*unavailable|bad.*gateway)",
            re.IGNORECASE,
        ),
        ErrorCategory.EXTERNAL_SERVICE,
        [
            ErrorDiagnostic(
                title="External Service Error",
                description="An external API or service returned an error response.",
                action="1. Check the status of the external service\n2. Review the API response for error details\n3. Implement retry logic for transient failures\n4. Check for rate limiting or quota issues",
                documentation_url=None,
            )
        ],
    ),
    # Configuration errors
    (
        re.compile(
            r"(config.*error|missing.*config|undefined.*variable|configuration.*not.*found)",
            re.IGNORECASE,
        ),
        ErrorCategory.CONFIGURATION,
        [
            ErrorDiagnostic(
                title="Configuration Error",
                description="The task failed due to missing or invalid configuration.",
                action="1. Check the Airflow variables and connections\n2. Verify all required configuration keys are set\n3. Review the DAG or operator configuration\n4. Ensure all required secrets are properly configured",
                documentation_url="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html",
            )
        ],
    ),
    # Python code errors
    (
        re.compile(
            r"(Traceback|Exception|Error|IndentationError|SyntaxError|ModuleNotFoundError|AttributeError)",
            re.IGNORECASE,
        ),
        ErrorCategory.CODE_ERROR,
        [
            ErrorDiagnostic(
                title="Code Execution Error",
                description="The Python code in the task raised an exception.",
                action="1. Check the task logs for the full traceback\n2. Review the exception type and message\n3. Check for typos or missing imports\n4. Verify the Python environment has required packages",
                documentation_url=None,
            )
        ],
    ),
]


def analyze_error(error_message: str | None) -> ErrorDiagnosticsResult:
    """
    Analyze an error message and provide diagnostic suggestions.

    Args:
        error_message: The error message from a failed task

    Returns:
        ErrorDiagnosticsResult containing category and diagnostic suggestions
    """
    if not error_message:
        return ErrorDiagnosticsResult(
            error_category=ErrorCategory.UNKNOWN,
            error_summary="No error message available",
            diagnostics=[],
        )

    # Truncate very long error messages for analysis
    analysis_text = error_message[:5000] if len(error_message) > 5000 else error_message

    # Find matching patterns
    for pattern, category, diagnostics in ERROR_PATTERNS:
        if pattern.search(analysis_text):
            # Extract a summary from the error message
            summary_lines = error_message.split("\n")
            summary = summary_lines[0][:200] if summary_lines else error_message[:200]

            return ErrorDiagnosticsResult(
                error_category=category,
                error_summary=summary,
                diagnostics=diagnostics,
            )

    # Default: unknown error
    summary_lines = error_message.split("\n")
    summary = summary_lines[0][:200] if summary_lines else error_message[:200]

    return ErrorDiagnosticsResult(
        error_category=ErrorCategory.UNKNOWN,
        error_summary=summary,
        diagnostics=[
            ErrorDiagnostic(
                title="Unknown Error",
                description="An unexpected error occurred. Please check the task logs for more details.",
                action="1. Review the full task logs for error details\n2. Check if this is a known issue with the operator\n3. Search for the error message online\n4. Consider adding custom error handling",
                documentation_url=None,
            )
        ],
    )


def get_error_category_display_name(category: str) -> str:
    """Get a human-readable display name for an error category."""
    category_names = {
        ErrorCategory.CONNECTION: "Connection Error",
        ErrorCategory.AUTHENTICATION: "Authentication Error",
        ErrorCategory.PERMISSION: "Permission Error",
        ErrorCategory.RESOURCE: "Resource Error",
        ErrorCategory.TIMEOUT: "Timeout Error",
        ErrorCategory.VALIDATION: "Validation Error",
        ErrorCategory.EXTERNAL_SERVICE: "External Service Error",
        ErrorCategory.CODE_ERROR: "Code Error",
        ErrorCategory.CONFIGURATION: "Configuration Error",
        ErrorCategory.UNKNOWN: "Unknown Error",
    }
    return category_names.get(category, "Unknown Error")
