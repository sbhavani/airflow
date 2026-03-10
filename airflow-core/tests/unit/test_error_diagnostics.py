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
"""Tests for error diagnostics module."""

import pytest

from airflow import error_diagnostics
from airflow.error_diagnostics import (
    ErrorDiagnostic,
    get_error_diagnostic,
    get_error_diagnostic_from_exception,
    get_error_type_from_exception,
)


class TestErrorDiagnostics:
    """Tests for error diagnostics module."""

    def test_get_error_diagnostic_airflow_task_timeout(self):
        """Test getting error diagnostic for AirflowTaskTimeout."""
        diagnostic = get_error_diagnostic("AirflowTaskTimeout")
        assert diagnostic is not None
        assert diagnostic.error_type == "AirflowTaskTimeout"
        assert len(diagnostic.possible_causes) > 0
        assert len(diagnostic.remediation_steps) > 0

    def test_get_error_diagnostic_airflow_exception(self):
        """Test getting error diagnostic for AirflowException."""
        diagnostic = get_error_diagnostic("AirflowException")
        assert diagnostic is not None
        assert diagnostic.error_type == "AirflowException"
        assert len(diagnostic.possible_causes) > 0
        assert len(diagnostic.remediation_steps) > 0

    def test_get_error_diagnostic_connection_error(self):
        """Test getting error diagnostic for ConnectionError."""
        diagnostic = get_error_diagnostic("ConnectionError")
        assert diagnostic is not None
        assert diagnostic.error_type == "ConnectionError"
        assert len(diagnostic.possible_causes) > 0
        assert len(diagnostic.remediation_steps) > 0

    def test_get_error_diagnostic_unknown(self):
        """Test getting error diagnostic for unknown error type."""
        diagnostic = get_error_diagnostic("UnknownError")
        assert diagnostic is None

    def test_get_error_type_from_exception_timeout(self):
        """Test extracting error type from TimeoutError."""
        error = TimeoutError("Operation timed out")
        error_type = get_error_type_from_exception(error)
        assert error_type == "TimeoutError"

    def test_get_error_type_from_exception_connection(self):
        """Test extracting error type from ConnectionError."""
        error = ConnectionError("Connection refused")
        error_type = get_error_type_from_exception(error)
        assert error_type == "ConnectionError"

    def test_get_error_type_from_exception_value(self):
        """Test extracting error type from ValueError."""
        error = ValueError("Invalid value")
        error_type = get_error_type_from_exception(error)
        assert error_type == "ValueError"

    def test_get_error_diagnostic_from_exception_none(self):
        """Test getting diagnostic from None exception."""
        diagnostic = get_error_diagnostic_from_exception(None)
        assert diagnostic is None

    def test_error_diagnostics_dict_contains_common_errors(self):
        """Test that ERROR_DIAGNOSTICS contains common error types."""
        expected_errors = [
            "AirflowFailException",
            "AirflowSensorTimeout",
            "AirflowTaskTimeout",
            "AirflowRuntimeError",
            "AirflowTaskTerminated",
            "AirflowException",
            "ConnectionError",
            "TimeoutError",
            "PermissionError",
            "FileNotFoundError",
            "KeyError",
            "ValueError",
            "ImportError",
            "OperationalError",
        ]
        for error_type in expected_errors:
            assert error_type in error_diagnostics.ERROR_DIAGNOSTICS

    def test_error_diagnostic_structure(self):
        """Test that all error diagnostics have the correct structure."""
        for error_type, diagnostic in error_diagnostics.ERROR_DIAGNOSTICS.items():
            assert isinstance(diagnostic, ErrorDiagnostic)
            assert diagnostic.error_type == error_type
            assert isinstance(diagnostic.possible_causes, (list, tuple))
            assert isinstance(diagnostic.remediation_steps, (list, tuple))
            assert len(diagnostic.possible_causes) > 0
            assert len(diagnostic.remediation_steps) > 0
            # Verify all causes and steps are strings
            for cause in diagnostic.possible_causes:
                assert isinstance(cause, str)
            for step in diagnostic.remediation_steps:
                assert isinstance(step, str)
