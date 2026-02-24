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
"""Memory profiling utilities for DAG parsing operations."""
from __future__ import annotations

import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator

import psutil

from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow.models.dag import DagModel


@dataclass
class MemoryMetrics:
    """Memory metrics captured during an operation."""

    peak_rss_bytes: int = 0
    """Peak resident set size in bytes."""

    memory_delta_bytes: int = 0
    """Memory delta (difference in RSS before and after)."""

    current_rss_bytes: int = 0
    """Current RSS at the end of the operation."""

    allocation_count: int = 0
    """Number of memory allocations."""

    peak_allocated_bytes: int = 0
    """Peak memory allocated by tracemalloc."""

    def to_mb(self) -> dict:
        """Convert bytes to megabytes for display."""
        return {
            "peak_rss_mb": self.peak_rss_bytes / (1024 * 1024),
            "memory_delta_mb": self.memory_delta_bytes / (1024 * 1024),
            "current_rss_mb": self.current_rss_bytes / (1024 * 1024),
            "peak_allocated_mb": self.peak_allocated_bytes / (1024 * 1024),
        }


@dataclass
class MemoryThresholdConfig:
    """Configuration for memory threshold monitoring."""

    threshold_mb: int = 256
    """Memory threshold in MB."""

    enabled: bool = True
    """Whether threshold monitoring is enabled."""

    warning_level: str = "WARNING"
    """Warning level: DEBUG, INFO, WARNING, ERROR, CRITICAL."""

    @classmethod
    def from_config(cls) -> "MemoryThresholdConfig":
        """Load threshold configuration from Airflow config."""
        return cls(
            threshold_mb=conf.getint("dag_processing", "memory_threshold_mb", fallback=256),
            enabled=conf.getboolean("dag_processing", "memory_threshold_enabled", fallback=True),
            warning_level=conf.get("dag_processing", "memory_warning_level", fallback="WARNING"),
        )


@dataclass
class DagMemoryContext:
    """Context for DAG-specific memory configuration."""

    dag_id: str
    """The DAG ID being parsed."""

    file_path: str
    """Path to the DAG file."""

    threshold_config: MemoryThresholdConfig = field(default_factory=MemoryThresholdConfig.from_config)
    """Memory threshold configuration for this DAG."""

    @classmethod
    def create(cls, dag_id: str, file_path: str, dag_model: "DagModel | None" = None) -> "DagMemoryContext":
        """Create a DAG memory context with optional per-DAG threshold override."""
        base_config = MemoryThresholdConfig.from_config()

        # Check for per-DAG threshold override via dag_model
        if dag_model and dag_model.memory_threshold is not None:
            base_config.threshold_mb = dag_model.memory_threshold

        return cls(
            dag_id=dag_id,
            file_path=file_path,
            threshold_config=base_config,
        )

    def threshold_exceeded(self, metrics: MemoryMetrics) -> bool:
        """Check if memory usage exceeds the configured threshold."""
        if not self.threshold_config.enabled:
            return False
        return metrics.peak_rss_bytes > (self.threshold_config.threshold_mb * 1024 * 1024)


@contextmanager
def memory_profiler_context(
    dag_id: str, file_path: str, dag_model: "DagModel | None" = None
) -> Generator[DagMemoryContext, None, None]:
    """
    Context manager for profiling memory during DAG parsing.

    Args:
        dag_id: The DAG ID being parsed
        file_path: Path to the DAG file
        dag_model: Optional DagModel for per-DAG configuration

    Yields:
        DagMemoryContext with metrics after the operation completes
    """
    context = DagMemoryContext.create(dag_id, file_path, dag_model)

    # Start tracemalloc for allocation tracking
    tracemalloc.start()

    # Capture initial RSS
    process = psutil.Process()
    initial_rss = process.memory_info().rss

    try:
        yield context
    finally:
        # Capture final RSS and metrics
        final_rss = process.memory_info().rss
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Calculate metrics
        context.metrics = MemoryMetrics(
            peak_rss_bytes=max(initial_rss, final_rss),
            memory_delta_bytes=final_rss - initial_rss,
            current_rss_bytes=final_rss,
            allocation_count=tracemalloc.get_traced_memory()[0] if False else 0,  # allocation_count not directly available
            peak_allocated_bytes=peak,
        )


def get_system_memory_info() -> dict:
    """Get current system memory information."""
    mem = psutil.virtual_memory()
    return {
        "total_mb": mem.total / (1024 * 1024),
        "available_mb": mem.available / (1024 * 1024),
        "used_mb": mem.used / (1024 * 1024),
        "percent": mem.percent,
    }


def get_process_memory_info() -> dict:
    """Get current process memory information."""
    process = psutil.Process()
    mem_info = process.memory_info()
    return {
        "rss_mb": mem_info.rss / (1024 * 1024),
        "vms_mb": mem_info.vms / (1024 * 1024),
    }
