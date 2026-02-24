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
"""Memory profiling utilities for DAG parsing."""
from __future__ import annotations

import contextlib
import os
import tracemalloc
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

from airflow.configuration import conf

if TYPE_CHECKING:
    from collections.abc import Generator

log = structlog.get_logger(__name__)

# Conversion factor: bytes to megabytes
BYTES_TO_MB = 1024 * 1024


@dataclass
class MemoryStats:
    """Memory statistics snapshot."""

    current_mb: float = 0.0
    peak_mb: float = 0.0
    delta_mb: float = 0.0

    def __str__(self) -> str:
        return f"current={self.current_mb:.2f}MB, peak={self.peak_mb:.2f}MB, delta={self.delta_mb:.2f}MB"


@dataclass
class MemoryMonitoringResult:
    """Result of memory monitoring check."""

    is_warning: bool = False
    message: str = ""
    memory_used_mb: float = 0.0
    limit_mb: float = 0.0
    threshold_percentage: int = 0


class MemoryProfiler:
    """
    Memory profiler for tracking memory usage during DAG parsing.

    This class provides utilities to monitor memory consumption during DAG parsing
    operations and generate warnings when configurable thresholds are exceeded.
    """

    def __init__(self) -> None:
        self._enabled: bool | None = None
        self._limit_per_dag_mb: int | None = None
        self._limit_total_mb: int | None = None
        self._warning_threshold_percent: int | None = None
        self._tracing_active: bool = False
        self._baseline_memory_mb: float = 0.0
        self._total_parsing_memory_mb: float = 0.0

    @property
    def enabled(self) -> bool:
        """Check if memory monitoring is enabled."""
        if self._enabled is None:
            self._enabled = conf.getboolean("dag_processor", "memory_monitoring_enabled")
        return self._enabled

    @property
    def limit_per_dag_mb(self) -> int:
        """Get the per-DAG memory limit in MB."""
        if self._limit_per_dag_mb is None:
            self._limit_per_dag_mb = conf.getint("dag_processor", "memory_limit_per_dag")
        return self._limit_per_dag_mb

    @property
    def limit_total_mb(self) -> int:
        """Get the total memory limit in MB."""
        if self._limit_total_mb is None:
            self._limit_total_mb = conf.getint("dag_processor", "memory_limit_total")
        return self._limit_total_mb

    @property
    def warning_threshold_percent(self) -> int:
        """Get the warning threshold percentage."""
        if self._warning_threshold_percent is None:
            self._warning_threshold_percent = conf.getint("dag_processor", "memory_warning_threshold")
        return self._warning_threshold_percent

    def start_monitoring(self) -> None:
        """Start memory monitoring by recording the baseline memory usage."""
        if not self.enabled:
            return

        if not self._tracing_active:
            tracemalloc.start()
            self._tracing_active = True

        # Record baseline memory
        current, _ = tracemalloc.get_traced_memory()
        self._baseline_memory_mb = current / BYTES_TO_MB
        self._total_parsing_memory_mb = 0.0

    def stop_monitoring(self) -> None:
        """Stop memory monitoring."""
        if self._tracing_active:
            tracemalloc.stop()
            self._tracing_active = False
            self._baseline_memory_mb = 0.0

    def get_current_memory(self) -> MemoryStats:
        """Get current memory statistics."""
        if not self.enabled or not self._tracing_active:
            return MemoryStats()

        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / BYTES_TO_MB
        peak_mb = peak / BYTES_TO_MB
        delta_mb = current_mb - self._baseline_memory_mb

        return MemoryStats(
            current_mb=current_mb,
            peak_mb=peak_mb,
            delta_mb=delta_mb,
        )

    def check_memory_limit(
        self,
        file_path: str | None = None,
        dag_count: int = 0,
    ) -> Generator[MemoryMonitoringResult, None, None]:
        """
        Check if memory usage exceeds configured limits.

        Yields warnings when memory limits are exceeded.

        Args:
            file_path: Path to the DAG file being parsed (for error messages)
            dag_count: Number of DAGs parsed so far

        Yields:
            MemoryMonitoringResult for each warning generated
        """
        if not self.enabled:
            return

        memory_stats = self.get_current_memory()

        # Check per-DAG memory limit
        if self.limit_per_dag_mb > 0 and memory_stats.delta_mb > 0:
            threshold_mb = (self.limit_per_dag_mb * self.warning_threshold_percent) / 100
            if memory_stats.delta_mb > threshold_mb:
                yield MemoryMonitoringResult(
                    is_warning=True,
                    message=(
                        f"Memory usage for DAG file '{file_path}' exceeded warning threshold. "
                        f"Used: {memory_stats.delta_mb:.2f}MB, "
                        f"Threshold ({self.warning_threshold_percent}%): {threshold_mb:.2f}MB, "
                        f"Limit: {self.limit_per_dag_mb}MB"
                    ),
                    memory_used_mb=memory_stats.delta_mb,
                    limit_mb=self.limit_per_dag_mb,
                    threshold_percentage=self.warning_threshold_percent,
                )

        # Check total memory limit
        if self.limit_total_mb > 0:
            threshold_total_mb = (self.limit_total_mb * self.warning_threshold_percent) / 100
            if memory_stats.current_mb > threshold_total_mb:
                yield MemoryMonitoringResult(
                    is_warning=True,
                    message=(
                        f"Total DAG parsing memory usage exceeded warning threshold. "
                        f"Used: {memory_stats.current_mb:.2f}MB, "
                        f"Threshold ({self.warning_threshold_percent}%): {threshold_total_mb:.2f}MB, "
                        f"Limit: {self.limit_total_mb}MB"
                    ),
                    memory_used_mb=memory_stats.current_mb,
                    limit_mb=self.limit_total_mb,
                    threshold_percentage=self.warning_threshold_percent,
                )


@contextlib.contextmanager
def memory_profiler_context(
    profiler: MemoryProfiler,
    file_path: str | None = None,
) -> Generator[list[MemoryMonitoringResult], None, None]:
    """
    Context manager for tracking memory during DAG parsing.

    This context manager automatically starts memory monitoring on entry,
    checks limits during execution, and yields any warnings generated.

    Args:
        profiler: The MemoryProfiler instance to use
        file_path: Path to the DAG file being parsed

    Yields:
        List of MemoryMonitoringResult warnings generated during parsing
    """
    warnings: list[MemoryMonitoringResult] = []

    if not profiler.enabled:
        yield warnings
        return

    profiler.start_monitoring()
    try:
        yield warnings
    finally:
        # Check memory after parsing is complete
        for result in profiler.check_memory_limit(file_path=file_path):
            warnings.append(result)
            log.warning("memory_limit_exceeded", **result.__dict__)
        profiler.stop_monitoring()


# Global memory profiler instance
_memory_profiler: MemoryProfiler | None = None


def get_memory_profiler() -> MemoryProfiler:
    """Get the global memory profiler instance."""
    global _memory_profiler
    if _memory_profiler is None:
        _memory_profiler = MemoryProfiler()
    return _memory_profiler
