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
"""Database models for user-defined metrics."""
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from airflow.models.base import Base, StringID
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from datetime import datetime


class AggregationFunction(str, Enum):
    """Aggregation functions for user-defined metrics."""

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    LAST = "last"


class UserDefinedMetric(Base):
    """
    Model for storing user-defined metrics emitted by operators.

    This table stores custom metrics that operators can emit during task execution.
    Metrics are associated with task instances and can be aggregated using
    various aggregation functions.
    """

    __tablename__ = "user_defined_metric"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False, index=True)
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False, index=True)
    map_index: Mapped[int] = mapped_column(Integer, nullable=False, server_default="-1")

    # Metric identification
    metric_name: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    metric_label: Mapped[str] = mapped_column(String(250), nullable=True, index=True)

    # Metric value
    value: Mapped[float] = mapped_column(Float, nullable=False)

    # Aggregation function to use when aggregating this metric
    aggregation: Mapped[str] = mapped_column(String(50), nullable=False, default=AggregationFunction.SUM.value)

    # Additional metadata
    source: Mapped[str] = mapped_column(String(100), nullable=False, default="operator")
    unit: Mapped[str] = mapped_column(String(50), nullable=True)

    # Timestamp
    timestamp: Mapped["datetime"] = mapped_column(nullable=False)

    # Task instance state when metric was emitted (to filter by success/failure)
    task_state: Mapped[str] = mapped_column(String(20), nullable=True)

    # Additional tags as JSON string
    tags: Mapped[str] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("dag_id", "task_id", "run_id", "map_index", "metric_name", "metric_label", name="udm_unique_metric"),
        Index("udm_metric_aggregation_idx", "dag_id", "task_id", "metric_name", "aggregation"),
        Index("udm_timestamp_idx", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<UserDefinedMetric(id={self.id}, dag_id={self.dag_id}, task_id={self.task_id}, "
            f"metric_name={self.metric_name}, value={self.value}, aggregation={self.aggregation})>"
        )
