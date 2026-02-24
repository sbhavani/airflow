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
"""Memory warning system for DAG parsing operations."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from airflow.configuration import conf
from airflow.utils.memory_profiler import DagMemoryContext, MemoryMetrics

if TYPE_CHECKING:
    from airflow.models.dagparsememory import DagParseMemoryMetric

log = structlog.get_logger(logger_name=__name__)


@dataclass
class MemoryWarning:
    """Warning generated when memory threshold is exceeded."""

    dag_id: str
    """The DAG ID that exceeded the threshold."""

    file_path: str
    """Path to the DAG file."""

    threshold_mb: int
    """The configured threshold in MB."""

    peak_memory_mb: float
    """Actual peak memory usage in MB."""

    memory_delta_mb: float
    """Memory delta in MB."""

    timestamp: datetime
    """When the warning was generated."""

    severity: str
    """Warning severity level."""

    message: str
    """Human-readable warning message."""

    def to_dict(self) -> dict:
        """Convert warning to dictionary for logging/display."""
        return {
            "dag_id": self.dag_id,
            "file_path": self.file_path,
            "threshold_mb": self.threshold_mb,
            "peak_memory_mb": self.peak_memory_mb,
            "memory_delta_mb": self.memory_delta_mb,
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity,
            "message": self.message,
        }


def generate_memory_warning(
    context: DagMemoryContext,
    metrics: MemoryMetrics,
) -> MemoryWarning:
    """Generate a memory warning when threshold is exceeded."""
    threshold_config = context.threshold_config
    metrics_mb = metrics.to_mb()

    message = (
        f"DAG '{context.dag_id}' exceeded memory threshold during parsing. "
        f"Peak memory: {metrics_mb['peak_rss_mb']:.2f}MB, "
        f"Threshold: {threshold_config.threshold_mb}MB, "
        f"File: {context.file_path}"
    )

    return MemoryWarning(
        dag_id=context.dag_id,
        file_path=context.file_path,
        threshold_mb=threshold_config.threshold_mb,
        peak_memory_mb=metrics_mb["peak_rss_mb"],
        memory_delta_mb=metrics_mb["memory_delta_mb"],
        timestamp=datetime.utcnow(),
        severity=threshold_config.warning_level,
        message=message,
    )


def emit_warning_log(warning: MemoryWarning) -> None:
    """Emit warning to logs based on configured severity level."""
    # Map severity string to logging level
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    log_level = level_map.get(warning.severity.upper(), logging.WARNING)
    log.log(
        log_level,
        "Memory threshold exceeded for DAG: dag_id=%s file=%s peak_memory_mb=%.2f threshold_mb=%d",
        warning.dag_id,
        warning.file_path,
        warning.peak_memory_mb,
        warning.threshold_mb,
        extra=warning.to_dict(),
    )


def should_emit_warning() -> bool:
    """Check if warnings are enabled in configuration."""
    return conf.getboolean("dag_processing", "memory_warnings_enabled", fallback=True)


def emit_warning(
    context: DagMemoryContext,
    metrics: MemoryMetrics,
) -> MemoryWarning | None:
    """
    Emit a memory warning if threshold is exceeded.

    Returns the warning if generated, None otherwise.
    """
    if not context.threshold_config.enabled:
        return None

    if not should_emit_warning():
        return None

    if not context.threshold_exceeded(metrics):
        return None

    warning = generate_memory_warning(context, metrics)
    emit_warning_log(warning)

    return warning


def store_warning_metrics(
    warning: MemoryWarning,
    session,
) -> "DagParseMemoryMetric":
    """Store warning and metrics to database."""
    from airflow.models.dagparsememory import DagParseMemoryMetric

    metric = DagParseMemoryMetric(
        dag_id=warning.dag_id,
        file_path=warning.file_path,
        parse_date=warning.timestamp,
        threshold_mb=warning.threshold_mb,
        peak_memory_mb=warning.peak_memory_mb,
        memory_delta_mb=warning.memory_delta_mb,
        threshold_exceeded=True,
    )
    session.add(metric)
    return metric
