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
"""Tests for memory profiling utilities."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.utils.memory_profiler import (
    MemoryMonitoringResult,
    MemoryProfiler,
    MemoryStats,
    get_memory_profiler,
    memory_profiler_context,
)

from tests_common.test_utils.config import conf_vars


class TestMemoryStats:
    """Test suite for MemoryStats dataclass."""

    def test_str_representation(self):
        """Test string representation of MemoryStats."""
        stats = MemoryStats(current_mb=100.5, peak_mb=150.3, delta_mb=50.2)
        result = str(stats)
        assert "current=100.50MB" in result
        assert "peak=150.30MB" in result
        assert "delta=50.20MB" in result


class TestMemoryMonitoringResult:
    """Test suite for MemoryMonitoringResult dataclass."""

    def test_default_values(self):
        """Test default values of MemoryMonitoringResult."""
        result = MemoryMonitoringResult()
        assert result.is_warning is False
        assert result.message == ""
        assert result.memory_used_mb == 0.0
        assert result.limit_mb == 0.0
        assert result.threshold_percentage == 0


class TestMemoryProfiler:
    """Test suite for MemoryProfiler class."""

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_enabled_with_config(self):
        """Test that profiler is enabled when configuration is set."""
        profiler = MemoryProfiler()
        assert profiler.enabled is True

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "False",
        }
    )
    def test_disabled_by_default(self):
        """Test that profiler is disabled when configuration is not set."""
        profiler = MemoryProfiler()
        assert profiler.enabled is False

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_limit_per_dag_config(self):
        """Test per-DAG memory limit configuration."""
        profiler = MemoryProfiler()
        assert profiler.limit_per_dag_mb == 512

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_limit_total_config(self):
        """Test total memory limit configuration."""
        profiler = MemoryProfiler()
        assert profiler.limit_total_mb == 2048

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_warning_threshold_config(self):
        """Test warning threshold configuration."""
        profiler = MemoryProfiler()
        assert profiler.warning_threshold_percent == 80

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_check_memory_limit_no_warning_when_disabled(self):
        """Test that no warning is generated when monitoring is disabled."""
        profiler = MemoryProfiler()
        # Even with memory stats, should not generate warnings when disabled
        results = list(profiler.check_memory_limit(file_path="test.py"))
        assert len(results) == 0

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "1",  # Very low limit for testing
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_check_memory_limit_per_dag_warning(self):
        """Test per-DAG memory limit warning."""
        profiler = MemoryProfiler()
        profiler._tracing_active = True
        profiler._baseline_memory_mb = 0.0

        # Mock get_current_memory to return high memory usage
        with patch.object(profiler, "get_current_memory") as mock_get_memory:
            mock_get_memory.return_value = MemoryStats(
                current_mb=100.0, peak_mb=100.0, delta_mb=1.0  # 1MB delta exceeds 0.8MB threshold
            )
            results = list(profiler.check_memory_limit(file_path="test.py"))
            assert len(results) == 1
            assert results[0].is_warning is True
            assert "test.py" in results[0].message

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "1",  # Very low limit for testing
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_check_memory_limit_total_warning(self):
        """Test total memory limit warning."""
        profiler = MemoryProfiler()
        profiler._tracing_active = True
        profiler._baseline_memory_mb = 0.0

        with patch.object(profiler, "get_current_memory") as mock_get_memory:
            mock_get_memory.return_value = MemoryStats(
                current_mb=1.0,  # 1MB exceeds 0.8MB threshold (80% of 1MB)
                peak_mb=1.0,
                delta_mb=1.0,
            )
            results = list(profiler.check_memory_limit(file_path="test.py"))
            assert len(results) == 1
            assert results[0].is_warning is True

    def test_get_current_memory_when_disabled(self):
        """Test get_current_memory returns empty stats when disabled."""
        profiler = MemoryProfiler()
        profiler._enabled = False
        stats = profiler.get_current_memory()
        assert stats.current_mb == 0.0
        assert stats.peak_mb == 0.0
        assert stats.delta_mb == 0.0


class TestGetMemoryProfiler:
    """Test suite for get_memory_profiler function."""

    def test_returns_same_instance(self):
        """Test that get_memory_profiler returns the same instance."""
        profiler1 = get_memory_profiler()
        profiler2 = get_memory_profiler()
        assert profiler1 is profiler2


class TestMemoryProfilerContext:
    """Test suite for memory_profiler_context context manager."""

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "False",
        }
    )
    def test_context_no_op_when_disabled(self):
        """Test that context manager yields empty list when disabled."""
        profiler = MemoryProfiler()
        with memory_profiler_context(profiler, "test.py") as warnings:
            assert warnings == []

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_context_starts_and_stops_monitoring(self):
        """Test that context manager starts and stops monitoring."""
        profiler = MemoryProfiler()
        with patch.object(profiler, "start_monitoring") as mock_start, patch.object(
            profiler, "stop_monitoring"
        ) as mock_stop, patch.object(profiler, "check_memory_limit") as mock_check:
            mock_check.return_value = []
            with memory_profiler_context(profiler, "test.py") as warnings:
                mock_start.assert_called_once()
            mock_stop.assert_called_once()

    @conf_vars(
        {
            ("dag_processor", "memory_monitoring_enabled"): "True",
            ("dag_processor", "memory_limit_per_dag"): "512",
            ("dag_processor", "memory_limit_total"): "2048",
            ("dag_processor", "memory_warning_threshold"): "80",
        }
    )
    def test_context_captures_warnings(self):
        """Test that context manager captures memory warnings."""
        profiler = MemoryProfiler()
        with patch.object(profiler, "start_monitoring"), patch.object(
            profiler, "stop_monitoring"
        ), patch.object(profiler, "check_memory_limit") as mock_check:
            mock_check.return_value = [
                MemoryMonitoringResult(
                    is_warning=True,
                    message="Test warning message",
                    memory_used_mb=100.0,
                    limit_mb=512.0,
                    threshold_percentage=80,
                )
            ]
            with memory_profiler_context(profiler, "test.py") as warnings:
                pass
            assert len(warnings) == 1
            assert warnings[0].message == "Test warning message"
