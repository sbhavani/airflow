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

"""Error diagnostics module for task failure analysis."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ErrorCategory(Enum):
    """Categories of task failures for error diagnostics."""

    CONNECTION = "CONNECTION"
    AUTHENTICATION = "AUTHENTICATION"
    CONFIGURATION = "CONFIGURATION"
    RESOURCE = "RESOURCE"
    UPSTREAM = "UPSTREAM"
    DATA = "DATA"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"


@dataclass
class PossibleCause:
    """Represents a possible cause of a task failure."""

    id: str
    description: str
    likelihood: str = "medium"  # high, medium, low


@dataclass
class RemediationStep:
    """Represents a remediation step for a task failure."""

    id: str
    description: str
    priority: int = 1
    documentation_link: str | None = None


@dataclass
class ErrorDiagnostics:
    """Structured error diagnostics for a failed task."""

    error_category: str
    error_type: str
    error_summary: str
    error_message: str
    possible_causes: list[dict[str, Any]] = field(default_factory=list)
    remediation_steps: list[dict[str, Any]] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# Error pattern mappings - maps regex patterns to error categories and types
ERROR_PATTERNS: list[tuple[str, ErrorCategory, str]] = [
    # Connection errors
    (r"ConnectionRefused", ErrorCategory.CONNECTION, "ConnectionRefused"),
    (r"ConnectTimeout", ErrorCategory.CONNECTION, "ConnectionTimeout"),
    (r"ConnectionError", ErrorCategory.CONNECTION, "ConnectionError"),
    (r"NewConnectionError", ErrorCategory.CONNECTION, "NewConnectionError"),
    (r"Remote end closed connection", ErrorCategory.CONNECTION, "ConnectionClosed"),
    # Authentication errors
    (r"AuthenticationError", ErrorCategory.AUTHENTICATION, "AuthenticationError"),
    (r"InvalidCredentials", ErrorCategory.AUTHENTICATION, "InvalidCredentials"),
    (r"PermissionDenied", ErrorCategory.AUTHENTICATION, "PermissionDenied"),
    (r"Unauthorized", ErrorCategory.AUTHENTICATION, "Unauthorized"),
    (r"403", ErrorCategory.AUTHENTICATION, "Forbidden"),
    # Configuration errors
    (r"ConfigError", ErrorCategory.CONNECTION, "ConfigError"),
    (r"MissingParam", ErrorCategory.CONFIGURATION, "MissingParameter"),
    (r"InvalidParam", ErrorCategory.CONFIGURATION, "InvalidParameter"),
    (r"RequiredParam", ErrorCategory.CONFIGURATION, "RequiredParameterMissing"),
    # Resource errors
    (r"MemoryError", ErrorCategory.RESOURCE, "OutOfMemory"),
    (r"DiskFull", ErrorCategory.RESOURCE, "DiskFull"),
    (r"CPULimit", ErrorCategory.RESOURCE, "CPULimitExceeded"),
    (r"MemoryLimit", ErrorCategory.RESOURCE, "MemoryLimitExceeded"),
    (r"ResourceWarning", ErrorCategory.RESOURCE, "ResourceWarning"),
    # Upstream errors
    (r"UpstreamFailed", ErrorCategory.UPSTREAM, "UpstreamTaskFailed"),
    (r"TriggerRuleNotMet", ErrorCategory.UPSTREAM, "TriggerRuleNotMet"),
    (r"TaskDeferred", ErrorCategory.UPSTREAM, "TaskDeferred"),
    # Data errors
    (r"ValueError", ErrorCategory.DATA, "ValueError"),
    (r"TypeError", ErrorCategory.DATA, "TypeError"),
    (r"ParseError", ErrorCategory.DATA, "ParseError"),
    (r"SchemaError", ErrorCategory.DATA, "SchemaError"),
    (r"ValidationError", ErrorCategory.DATA, "ValidationError"),
    # Timeout errors
    (r"TimeoutError", ErrorCategory.TIMEOUT, "TimeoutError"),
    (r"TaskTimeout", ErrorCategory.TIMEOUT, "TaskTimeout"),
    (r"ReadTimeout", ErrorCategory.TIMEOUT, "ReadTimeout"),
    (r"ConnectTimeout", ErrorCategory.TIMEOUT, "ConnectTimeout"),
]


def classify_error(error_message: str) -> tuple[ErrorCategory, str]:
    """
    Classify an error message into a category and type.

    Args:
        error_message: The raw error message or exception string

    Returns:
        Tuple of (ErrorCategory, error_type)
    """
    for pattern, category, error_type in ERROR_PATTERNS:
        if re.search(pattern, error_message, re.IGNORECASE):
            return category, error_type

    # Check for common Python exceptions
    if "Exception" in error_message or "Error" in error_message:
        # Extract exception type from the message
        match = re.search(r"(\w+Exception|\w+Error)", error_message)
        if match:
            return ErrorCategory.UNKNOWN, match.group(1)

    return ErrorCategory.UNKNOWN, "UnknownError"


def generate_error_summary(category: ErrorCategory, error_type: str, error_message: str) -> str:
    """Generate a human-readable error summary."""
    summaries = {
        ErrorCategory.CONNECTION: {
            "ConnectionRefused": "Connection Refused - The remote server refused the connection",
            "ConnectionTimeout": "Connection Timeout - The task failed to connect to the external service",
            "ConnectionError": "Connection Error - A connection error occurred",
            "ConnectionClosed": "Connection Closed - The remote end closed the connection unexpectedly",
        },
        ErrorCategory.AUTHENTICATION: {
            "AuthenticationError": "Authentication Failed - Invalid credentials provided",
            "InvalidCredentials": "Invalid Credentials - The credentials are not valid",
            "PermissionDenied": "Permission Denied - Access to the resource was denied",
            "Unauthorized": "Unauthorized - Authentication is required",
            "Forbidden": "Forbidden - Access to this resource is not allowed",
        },
        ErrorCategory.CONFIGURATION: {
            "ConfigError": "Configuration Error - Invalid task configuration",
            "MissingParameter": "Missing Parameter - A required parameter was not provided",
            "InvalidParameter": "Invalid Parameter - A parameter has an invalid value",
            "RequiredParameterMissing": "Required Parameter Missing - A required configuration value is missing",
        },
        ErrorCategory.RESOURCE: {
            "OutOfMemory": "Out of Memory - The task exceeded available memory",
            "DiskFull": "Disk Full - No space left on device",
            "CPULimitExceeded": "CPU Limit Exceeded - Task used more CPU than allowed",
            "MemoryLimitExceeded": "Memory Limit Exceeded - Task used more memory than allowed",
            "ResourceWarning": "Resource Warning - System resource limit approached",
        },
        ErrorCategory.UPSTREAM: {
            "UpstreamTaskFailed": "Upstream Task Failed - This task depends on a failed task",
            "TriggerRuleNotMet": "Trigger Rule Not Met - Required dependencies not satisfied",
            "TaskDeferred": "Task Deferred - Task was deferred to a trigger",
        },
        ErrorCategory.DATA: {
            "ValueError": "Value Error - Invalid value provided",
            "TypeError": "Type Error - Wrong data type encountered",
            "ParseError": "Parse Error - Failed to parse data",
            "SchemaError": "Schema Error - Data does not match expected schema",
            "ValidationError": "Validation Error - Data validation failed",
        },
        ErrorCategory.TIMEOUT: {
            "TimeoutError": "Timeout Error - Operation took too long",
            "TaskTimeout": "Task Timeout - Task exceeded its timeout",
            "ReadTimeout": "Read Timeout - Reading data took too long",
            "ConnectTimeout": "Connect Timeout - Connection attempt timed out",
        },
    }

    category_summaries = summaries.get(category, {})
    if error_type in category_summaries:
        return category_summaries[error_type]

    # Default summary
    return f"{category.value} Error - {error_type}"
