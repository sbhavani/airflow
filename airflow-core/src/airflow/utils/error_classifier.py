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

"""Error classifier module for categorizing task failures."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import yaml

from airflow.utils.error_diagnostics import ErrorCategory

logger = logging.getLogger(__name__)


@dataclass
class ErrorPattern:
    """Represents a single error pattern for classification."""

    pattern: str
    category: ErrorCategory
    error_type: str
    priority: int = 100  # Lower priority = checked first


@dataclass
class ClassificationResult:
    """Result of error classification."""

    category: ErrorCategory
    error_type: str
    matched_pattern: str | None = None
    confidence: float = 1.0


class ErrorClassifier:
    """
    Error classifier for categorizing task failures.

    Supports pattern-based classification with configurable patterns
    loaded from YAML configuration files. Also supports custom pattern
    registration for extensibility.
    """

    def __init__(self):
        self._patterns: list[ErrorPattern] = []
        self._compiled_patterns: list[tuple[re.Pattern, ErrorPattern]] = []
        self._custom_classifiers: list[Callable[[str], ClassificationResult | None]] = []
        self._config_dir: str | None = None
        self._initialized = False

    def initialize(self, config_dir: str | None = None) -> None:
        """
        Initialize the classifier with built-in and config patterns.

        Args:
            config_dir: Optional path to error_diagnostics config directory.
        """
        if self._initialized:
            return

        # Add built-in patterns
        self._add_builtin_patterns()

        # Load patterns from config files if directory provided
        if config_dir:
            self._config_dir = config_dir
            self._load_patterns_from_config()

        # Compile patterns for efficient matching
        self._compile_patterns()

        self._initialized = True

    def _add_builtin_patterns(self) -> None:
        """Add built-in error patterns for common errors."""
        builtin_patterns = [
            # Connection errors
            ErrorPattern(r"ConnectionRefused", ErrorCategory.CONNECTION, "ConnectionRefused", priority=10),
            ErrorPattern(r"ConnectTimeout", ErrorCategory.CONNECTION, "ConnectionTimeout", priority=10),
            ErrorPattern(r"ConnectionError", ErrorCategory.CONNECTION, "ConnectionError", priority=20),
            ErrorPattern(r"NewConnectionError", ErrorCategory.CONNECTION, "NewConnectionError", priority=20),
            ErrorPattern(r"Remote end closed connection", ErrorCategory.CONNECTION, "ConnectionClosed", priority=30),
            ErrorPattern(r"HTTP Error 5[0-9][0-9]", ErrorCategory.CONNECTION, "HttpServerError", priority=40),
            ErrorPattern(r"HTTPSConnectionPool", ErrorCategory.CONNECTION, "ConnectionPoolExhausted", priority=50),
            ErrorPattern(r"Max retries exceeded", ErrorCategory.CONNECTION, "MaxRetriesExceeded", priority=50),
            ErrorPattern(r"Connection reset by peer", ErrorCategory.CONNECTION, "ConnectionReset", priority=50),
            ErrorPattern(r"Name or service not known", ErrorCategory.CONNECTION, "DNSS resolutionFailed", priority=50),
            ErrorPattern(r"Could not resolve host", ErrorCategory.CONNECTION, "HostResolutionFailed", priority=50),
            ErrorPattern(r"Network is unreachable", ErrorCategory.CONNECTION, "NetworkUnreachable", priority=50),
            ErrorPattern(r"No route to host", ErrorCategory.CONNECTION, "NoRouteToHost", priority=50),

            # Authentication errors
            ErrorPattern(r"AuthenticationError", ErrorCategory.AUTHENTICATION, "AuthenticationError", priority=10),
            ErrorPattern(r"InvalidCredentials", ErrorCategory.AUTHENTICATION, "InvalidCredentials", priority=20),
            ErrorPattern(r"PermissionDenied", ErrorCategory.AUTHENTICATION, "PermissionDenied", priority=20),
            ErrorPattern(r"Unauthorized", ErrorCategory.AUTHENTICATION, "Unauthorized", priority=20),
            ErrorPattern(r"403\s+Forbidden", ErrorCategory.AUTHENTICATION, "Forbidden", priority=30),
            ErrorPattern(r"Auth.*failed", ErrorCategory.AUTHENTICATION, "AuthFailed", priority=30),
            ErrorPattern(r"Invalid token", ErrorCategory.AUTHENTICATION, "InvalidToken", priority=40),
            ErrorPattern(r"Token expired", ErrorCategory.AUTHENTICATION, "TokenExpired", priority=40),
            ErrorPattern(r"API key.*invalid", ErrorCategory.AUTHENTICATION, "InvalidApiKey", priority=40),

            # Configuration errors
            ErrorPattern(r"ConfigError", ErrorCategory.CONFIGURATION, "ConfigError", priority=10),
            ErrorPattern(r"MissingParam", ErrorCategory.CONFIGURATION, "MissingParameter", priority=20),
            ErrorPattern(r"InvalidParam", ErrorCategory.CONFIGURATION, "InvalidParameter", priority=20),
            ErrorPattern(r"RequiredParam", ErrorCategory.CONFIGURATION, "RequiredParameterMissing", priority=20),
            ErrorPattern(r"Configuration.*not found", ErrorCategory.CONFIGURATION, "ConfigNotFound", priority=30),
            ErrorPattern(r"Invalid configuration", ErrorCategory.CONFIGURATION, "InvalidConfiguration", priority=30),
            ErrorPattern(r"未找到配置", ErrorCategory.CONFIGURATION, "ConfigNotFound", priority=100),  # Chinese
            ErrorPattern(r"配置错误", ErrorCategory.CONFIGURATION, "ConfigError", priority=100),  # Chinese

            # Resource errors
            ErrorPattern(r"MemoryError", ErrorCategory.RESOURCE, "OutOfMemory", priority=10),
            ErrorPattern(r"Out of memory", ErrorCategory.RESOURCE, "OutOfMemory", priority=10),
            ErrorPattern(r"DiskFull", ErrorCategory.RESOURCE, "DiskFull", priority=20),
            ErrorPattern(r"No space left on device", ErrorCategory.RESOURCE, "DiskFull", priority=20),
            ErrorPattern(r"CPULimit", ErrorCategory.RESOURCE, "CPULimitExceeded", priority=30),
            ErrorPattern(r"MemoryLimit", ErrorCategory.RESOURCE, "MemoryLimitExceeded", priority=30),
            ErrorPattern(r"ResourceWarning", ErrorCategory.RESOURCE, "ResourceWarning", priority=40),
            ErrorPattern(r"OOMKilled", ErrorCategory.RESOURCE, "ContainerOomKilled", priority=20),
            ErrorPattern(r" Killed$", ErrorCategory.RESOURCE, "ProcessKilled", priority=50),
            ErrorPattern(r"File descriptor limit", ErrorCategory.RESOURCE, "FdLimitExceeded", priority=50),

            # Upstream errors
            ErrorPattern(r"UpstreamFailed", ErrorCategory.UPSTREAM, "UpstreamTaskFailed", priority=10),
            ErrorPattern(r"Upstream task.*failed", ErrorCategory.UPSTREAM, "UpstreamTaskFailed", priority=10),
            ErrorPattern(r"TriggerRuleNotMet", ErrorCategory.UPSTREAM, "TriggerRuleNotMet", priority=10),
            ErrorPattern(r"NoneType.*object has no attribute", ErrorCategory.UPSTREAM, "AttributeErrorOnNone", priority=30),
            ErrorPattern(r"cannot be null", ErrorCategory.UPSTREAM, "NullValueError", priority=30),
            ErrorPattern(r"upstream.*run_id", ErrorCategory.UPSTREAM, "UpstreamRunIdMismatch", priority=50),

            # Data errors
            ErrorPattern(r"ValueError", ErrorCategory.DATA, "ValueError", priority=10),
            ErrorPattern(r"TypeError", ErrorCategory.DATA, "TypeError", priority=10),
            ErrorPattern(r"ParseError", ErrorCategory.DATA, "ParseError", priority=20),
            ErrorPattern(r"JSONDecodeError", ErrorCategory.DATA, "JsonDecodeError", priority=20),
            ErrorPattern(r"SchemaError", ErrorCategory.DATA, "SchemaError", priority=30),
            ErrorPattern(r"ValidationError", ErrorCategory.DATA, "ValidationError", priority=30),
            ErrorPattern(r"Row.*error", ErrorCategory.DATA, "RowError", priority=40),
            ErrorPattern(r"Duplicate key", ErrorCategory.DATA, "DuplicateKeyError", priority=40),
            ErrorPattern(r"Foreign key.*violation", ErrorCategory.DATA, "ForeignKeyViolation", priority=50),
            ErrorPattern(r"Data too long", ErrorCategory.DATA, "DataTruncation", priority=50),
            ErrorPattern(r"NoneType.*is not iterable", ErrorCategory.DATA, "NoneNotIterable", priority=30),
            ErrorPattern(r"'NoneType' object", ErrorCategory.DATA, "NoneTypeError", priority=30),

            # Timeout errors
            ErrorPattern(r"TimeoutError", ErrorCategory.TIMEOUT, "TimeoutError", priority=10),
            ErrorPattern(r"TaskTimeout", ErrorCategory.TIMEOUT, "TaskTimeout", priority=10),
            ErrorPattern(r"ReadTimeout", ErrorCategory.TIMEOUT, "ReadTimeout", priority=20),
            ErrorPattern(r"ConnectTimeout", ErrorCategory.TIMEOUT, "ConnectTimeout", priority=20),
            ErrorPattern(r"Request timeout", ErrorCategory.TIMEOUT, "RequestTimeout", priority=30),
            ErrorPattern(r"timed out", ErrorCategory.TIMEOUT, "OperationTimedOut", priority=30,),
            ErrorPattern(r"timeout.*exceeded", ErrorCategory.TIMEOUT, "TimeoutExceeded", priority=40),

            # Python-specific errors
            ErrorPattern(r"ImportError", ErrorCategory.CONFIGURATION, "ImportError", priority=10),
            ErrorPattern(r"ModuleNotFoundError", ErrorCategory.CONFIGURATION, "ModuleNotFoundError", priority=10),
            ErrorPattern(r"AttributeError", ErrorCategory.DATA, "AttributeError", priority=20),
            ErrorPattern(r"KeyError", ErrorCategory.DATA, "KeyError", priority=20),
            ErrorPattern(r"IndexError", ErrorCategory.DATA, "IndexError", priority=20),
            ErrorPattern(r"FileNotFoundError", ErrorCategory.RESOURCE, "FileNotFoundError", priority=30),
            ErrorPattern(r"PermissionError", ErrorCategory.AUTHENTICATION, "PermissionError", priority=30),
            ErrorPattern(r"OSError", ErrorCategory.RESOURCE, "OsError", priority=40),

            # Airflow-specific errors
            ErrorPattern(r"AirflowException", ErrorCategory.UNKNOWN, "AirflowException", priority=50),
            ErrorPattern(r"DagNotFound", ErrorCategory.CONFIGURATION, "DagNotFound", priority=30),
            ErrorPattern(r"TaskNotFound", ErrorCategory.CONFIGURATION, "TaskNotFound", priority=30),
            ErrorPattern(r"PoolNotFound", ErrorCategory.CONFIGURATION, "PoolNotFound", priority=30),
            ErrorPattern(r"CeleryExecutor.*failed", ErrorCategory.RESOURCE, "CeleryExecutorFailed", priority=40),
            ErrorPattern(r"Kubernetes.*failed", ErrorCategory.RESOURCE, "KubernetesJobFailed", priority=40),
        ]

        self._patterns.extend(builtin_patterns)

    def _load_patterns_from_config(self) -> None:
        """Load additional patterns from YAML config files."""
        if not self._config_dir or not os.path.isdir(self._config_dir):
            return

        category_files = [
            "connection.yaml",
            "authentication.yaml",
            "configuration.yaml",
            "resource.yaml",
            "upstream.yaml",
            "data.yaml",
            "timeout.yaml",
            "unknown.yaml",
        ]

        for filename in category_files:
            filepath = os.path.join(self._config_dir, filename)
            if not os.path.exists(filepath):
                continue

            try:
                with open(filepath, "r") as f:
                    config = yaml.safe_load(f)

                if not config:
                    continue

                category = config.get("error_category")
                if not category:
                    continue

                # Map category string to enum
                try:
                    error_category = ErrorCategory(category)
                except ValueError:
                    logger.warning(f"Unknown error category in {filename}: {category}")
                    continue

                # Load patterns from config
                patterns = config.get("error_patterns", [])
                for pattern_config in patterns:
                    pattern = pattern_config.get("pattern", "")
                    error_type = pattern_config.get("error_type", "Unknown")

                    if pattern:
                        self._patterns.append(
                            ErrorPattern(
                                pattern=pattern,
                                category=error_category,
                                error_type=error_type,
                                priority=50,  # Config patterns have medium priority
                            )
                        )

            except Exception as e:
                logger.warning(f"Failed to load patterns from {filepath}: {e}")

    def _compile_patterns(self) -> None:
        """Compile all patterns for efficient matching."""
        self._compiled_patterns = []
        for pattern in sorted(self._patterns, key=lambda p: p.priority):
            try:
                compiled = re.compile(pattern.pattern, re.IGNORECASE | re.MULTILINE)
                self._compiled_patterns.append((compiled, pattern))
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern.pattern}': {e}")

    def add_pattern(self, pattern: str, category: ErrorCategory, error_type: str, priority: int = 100) -> None:
        """
        Add a custom error pattern.

        Args:
            pattern: Regex pattern to match
            category: Error category
            error_type: Specific error type
            priority: Pattern priority (lower = checked first)
        """
        error_pattern = ErrorPattern(
            pattern=pattern,
            category=category,
            error_type=error_type,
            priority=priority,
        )
        self._patterns.append(error_pattern)
        # Re-compile patterns
        self._compile_patterns()

    def add_custom_classifier(
        self, classifier: Callable[[str], ClassificationResult | None]
    ) -> None:
        """
        Add a custom classifier function.

        The classifier should take an error message string and return
        a ClassificationResult if it can classify the error, or None
        if it cannot.

        Args:
            classifier: Custom classification function
        """
        self._custom_classifiers.append(classifier)

    def classify(self, error_message: str) -> ClassificationResult:
        """
        Classify an error message into a category and type.

        Args:
            error_message: The raw error message or exception string

        Returns:
            ClassificationResult with category, error_type, and match info
        """
        if not self._initialized:
            self.initialize()

        # Try custom classifiers first (they have highest priority)
        for classifier in self._custom_classifiers:
            result = classifier(error_message)
            if result is not None:
                return result

        # Try pattern matching
        for compiled_pattern, pattern in self._compiled_patterns:
            match = compiled_pattern.search(error_message)
            if match:
                return ClassificationResult(
                    category=pattern.category,
                    error_type=pattern.error_type,
                    matched_pattern=pattern.pattern,
                    confidence=1.0,
                )

        # Fallback: try to extract exception type from message
        if "Exception" in error_message or "Error" in error_message:
            match = re.search(r"(\w+Exception|\w+Error)", error_message)
            if match:
                return ClassificationResult(
                    category=ErrorCategory.UNKNOWN,
                    error_type=match.group(1),
                    confidence=0.5,
                )

        # Ultimate fallback
        return ClassificationResult(
            category=ErrorCategory.UNKNOWN,
            error_type="UnknownError",
            confidence=0.1,
        )

    def get_categories(self) -> list[ErrorCategory]:
        """Get list of all supported error categories."""
        return list(ErrorCategory)


# Global classifier instance
_classifier: ErrorClassifier | None = None


def get_classifier() -> ErrorClassifier:
    """Get the global ErrorClassifier instance."""
    global _classifier
    if _classifier is None:
        _classifier = ErrorClassifier()
        # Initialize with default config path
        config_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "config",
            "error_diagnostics",
        )
        _classifier.initialize(config_dir)
    return _classifier


def classify_error(error_message: str) -> tuple[ErrorCategory, str]:
    """
    Convenience function to classify an error message.

    Args:
        error_message: The raw error message or exception string

    Returns:
        Tuple of (ErrorCategory, error_type)
    """
    result = get_classifier().classify(error_message)
    return result.category, result.error_type
