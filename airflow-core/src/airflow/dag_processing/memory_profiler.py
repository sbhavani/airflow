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

import resource
import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from airflow._shared.observability.metrics.stats import Stats
from airflow.configuration import conf

if TYPE_CHECKING:
    from collections.abc import MutableSequence

logger = structlog.get_logger(__name__)

# Default values
DEFAULT_MEMORY_THRESHOLD = 268435456  # 256 MB
DEFAULT_RATE_LIMIT_SECONDS = 300  # 5 minutes
MAX_PROFILE_RESULTS = 1000  # Keep last 1000 results


@dataclass
class MemoryThresholdConfig:
    """Configuration for memory monitoring thresholds."""

    enabled: bool = False
    threshold_bytes: int = DEFAULT_MEMORY_THRESHOLD
    rate_limit_window_seconds: int = DEFAULT_RATE_LIMIT_SECONDS


@dataclass
class MemoryProfileResult:
    """Memory usage data collected during a single DAG file parse operation."""

    dag_file_path: str
    timestamp: datetime
    memory_used_bytes: int
    peak_memory_bytes: int
    parsing_duration_ms: int
    exceeded_threshold: bool = False


class MemoryProfiler:
    """Handles memory profiling during DAG parsing."""

    def __init__(self, config: MemoryThresholdConfig):
        """Initialize with threshold configuration."""
        self.config = config
        self._tracemalloc_started = False
        self._start_time: float = 0
        self._start_memory: tuple[int, int] = (0, 0)

    def start_profiling(self) -> None:
        """Start tracking memory before DAG parse."""
        if not self.config.enabled:
            return

        # Use tracemalloc for lightweight Python memory tracking
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._tracemalloc_started = True

        self._start_time = time.perf_counter()
        self._start_memory = tracemalloc.get_traced_memory()

    def end_profiling(self, dag_file_path: str) -> MemoryProfileResult | None:
        """End tracking and return profile result."""
        if not self.config.enabled:
            return None

        end_time = time.perf_counter()
        end_memory = tracemalloc.get_traced_memory()

        # Calculate memory delta
        current, peak = end_memory
        start_current, start_peak = self._start_memory
        memory_used = current - start_current
        peak_memory = peak - start_peak

        # Also get RSS from system for more accurate measurement
        try:
            rus = resource.getrusage(resource.RUSAGE_SELF)
            # RSS is in kilobytes on Linux, bytes on macOS
            import platform

            system_rss = rus.ru_maxrss
            if platform.system() == "Linux":
                system_rss *= 1024  # Convert KB to bytes
        except Exception:
            system_rss = memory_used

        # Use the larger of the two measurements
        memory_used = max(memory_used, system_rss)

        parsing_duration_ms = int((end_time - self._start_time) * 1000)
        exceeded = memory_used > self.config.threshold_bytes

        result = MemoryProfileResult(
            dag_file_path=dag_file_path,
            timestamp=datetime.now(),
            memory_used_bytes=memory_used,
            peak_memory_bytes=peak_memory,
            parsing_duration_ms=parsing_duration_ms,
            exceeded_threshold=exceeded,
        )

        # Clean up tracemalloc if we started it
        if self._tracemalloc_started:
            tracemalloc.stop()
            self._tracemalloc_started = False

        return result

    def check_threshold(self, result: MemoryProfileResult) -> bool:
        """Check if result exceeded configured threshold."""
        return result.memory_used_bytes > self.config.threshold_bytes


# Global state for rate limiting and results storage
_warning_last_emitted: dict[str, float] = {}
_profile_results: MutableSequence[MemoryProfileResult] = []
_config_cache: tuple[str, MemoryThresholdConfig] | None = None


def get_memory_threshold_config() -> MemoryThresholdConfig:
    """Read memory threshold configuration from Airflow config with caching."""
    global _config_cache

    # Simple cache key based on config values
    cache_key = f"{conf.get('dag_processor', 'memory_profiling_enabled', fallback='False')}:{conf.get('dag_processor', 'memory_threshold', fallback=str(DEFAULT_MEMORY_THRESHOLD))}:{conf.get('dag_processor', 'memory_warning_rate_limit_seconds', fallback=str(DEFAULT_RATE_LIMIT_SECONDS))}"

    if _config_cache is None or _config_cache[0] != cache_key:
        enabled = conf.get_boolean("dag_processor", "memory_profiling_enabled", fallback=False)
        threshold = conf.get_int("dag_processor", "memory_threshold", fallback=DEFAULT_MEMORY_THRESHOLD)
        rate_limit = conf.get_int(
            "dag_processor", "memory_warning_rate_limit_seconds", fallback=DEFAULT_RATE_LIMIT_SECONDS
        )

        _config_cache = (cache_key, MemoryThresholdConfig(enabled=enabled, threshold_bytes=threshold, rate_limit_window_seconds=rate_limit))

    return _config_cache[1]


def should_emit_warning(dag_file_path: str) -> bool:
    """Check if warning should be emitted based on rate limiting."""
    config = get_memory_threshold_config()
    current_time = time.time()

    last_emitted = _warning_last_emitted.get(dag_file_path, 0)
    time_since_last = current_time - last_emitted

    if time_since_last < config.rate_limit_window_seconds:
        return False

    # Update last emitted time
    _warning_last_emitted[dag_file_path] = current_time
    return True


def emit_memory_metrics(result: MemoryProfileResult) -> None:
    """Emit memory metrics to Airflow stats system."""
    dag_file = result.dag_file_path

    # Emit gauge for memory usage
    Stats.gauge("dag_processor.memory.usage", result.memory_used_bytes, tags={"dag_file": dag_file})

    # Emit counter if threshold exceeded
    if result.exceeded_threshold:
        Stats.incr("dag_processor.memory.threshold_exceeded", tags={"dag_file": dag_file})

    # Emit histogram for parsing duration
    Stats.histogram("dag_processor.memory.parsing_duration", result.parsing_duration_ms, tags={"dag_file": dag_file})


def emit_memory_warning(result: MemoryProfileResult, threshold: int) -> None:
    """Emit warning log when threshold exceeded."""
    if not should_emit_warning(result.dag_file_path):
        return

    increase_pct = ((result.memory_used_bytes - threshold) / threshold * 100) if threshold > 0 else 0

    logger.warning(
        "DAG file exceeded memory threshold",
        dag_file=result.dag_file_path,
        memory_used_bytes=result.memory_used_bytes,
        threshold_bytes=threshold,
        increase_percent=round(increase_pct, 2),
    )


def store_profile_result(result: MemoryProfileResult) -> None:
    """Store profile result for historical access."""
    global _profile_results

    _profile_results.append(result)

    # Trim to max size
    while len(_profile_results) > MAX_PROFILE_RESULTS:
        _profile_results.pop(0)


def get_recent_profiles(limit: int = 100) -> list[MemoryProfileResult]:
    """Get recent memory profiling results."""
    return list(_profile_results[-limit:])


def get_peak_memory(dag_file_path: str) -> int | None:
    """Get peak memory usage for a specific DAG file."""
    for result in reversed(_profile_results):
        if result.dag_file_path == dag_file_path:
            return result.peak_memory_bytes
    return None
