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
"""Models for DAG parsing memory metrics."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Float, ForeignKeyConstraint, Index, Integer, String, Text, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.models.dag import DagModel

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class DagParseMemoryMetric(Base):
    """
    A table to store memory metrics from DAG parsing operations.

    This table records memory usage information each time a DAG file is parsed,
    enabling administrators to track memory consumption patterns and identify
    memory-intensive DAGs.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    """The DAG ID parsed."""

    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    """Path to the DAG file that was parsed."""

    parse_date: Mapped[datetime] = mapped_column(nullable=False, default=timezone.utcnow)
    """Timestamp when the DAG was parsed."""

    threshold_mb: Mapped[int] = mapped_column(Integer, nullable=False)
    """The memory threshold in MB that was applied."""

    peak_memory_mb: Mapped[float] = mapped_column(Float, nullable=False)
    """Peak memory usage in MB during parsing."""

    memory_delta_mb: Mapped[float] = mapped_column(Float, nullable=False)
    """Memory delta (change in RSS) in MB during parsing."""

    threshold_exceeded: Mapped[bool] = mapped_column(nullable=False, default=False)
    """Whether the memory threshold was exceeded during this parse."""

    dag_model = relationship("DagModel", viewonly=True, lazy="selectin")

    __tablename__ = "dag_parse_memory_metric"
    __table_args__ = (
        ForeignKeyConstraint(
            ("dag_id",),
            ["dag.dag_id"],
            name="dpmm_dag_id_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_dag_parse_memory_metric_dag_id", dag_id),
        Index("idx_dag_parse_memory_metric_parse_date", parse_date),
    )

    def __init__(
        self,
        dag_id: str,
        file_path: str,
        parse_date: datetime | None = None,
        threshold_mb: int = 256,
        peak_memory_mb: float = 0.0,
        memory_delta_mb: float = 0.0,
        threshold_exceeded: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dag_id = dag_id
        self.file_path = file_path
        self.parse_date = parse_date or timezone.utcnow()
        self.threshold_mb = threshold_mb
        self.peak_memory_mb = peak_memory_mb
        self.memory_delta_mb = memory_delta_mb
        self.threshold_exceeded = threshold_exceeded

    @classmethod
    def get_memory_metrics(
        cls,
        session: Session,
        dag_id: str | None = None,
        file_path: str | None = None,
        parse_date_start: datetime | None = None,
        parse_date_end: datetime | None = None,
        limit: int = 100,
    ) -> list[DagParseMemoryMetric]:
        """
        Query memory metrics with optional filters.

        Args:
            session: Database session
            dag_id: Optional DAG ID to filter by
            file_path: Optional file path to filter by
            parse_date_start: Optional start date for filtering
            parse_date_end: Optional end date for filtering
            limit: Maximum number of results to return

        Returns:
            List of DagParseMemoryMetric records
        """
        query = select(cls)

        if dag_id:
            query = query.where(cls.dag_id == dag_id)
        if file_path:
            query = query.where(cls.file_path == file_path)
        if parse_date_start:
            query = query.where(cls.parse_date >= parse_date_start)
        if parse_date_end:
            query = query.where(cls.parse_date <= parse_date_end)

        query = query.order_by(cls.parse_date.desc()).limit(limit)

        return session.execute(query).scalars().all()

    @classmethod
    def get_dags_exceeding_threshold(
        cls,
        session: Session,
        days: int = 30,
    ) -> list[DagParseMemoryMetric]:
        """
        Get DAGs that have exceeded the memory threshold.

        Args:
            session: Database session
            days: Number of days to look back

        Returns:
            List of DagParseMemoryMetric records where threshold was exceeded
        """
        from datetime import timedelta

        cutoff_date = datetime.utcnow() - timedelta(days=days)

        query = (
            select(cls)
            .where(cls.threshold_exceeded == True)  # noqa: E712
            .where(cls.parse_date >= cutoff_date)
            .order_by(cls.peak_memory_mb.desc())
        )

        return session.execute(query).scalars().all()

    @classmethod
    def get_average_memory_by_dag(
        cls,
        session: Session,
        days: int = 30,
    ) -> list[dict]:
        """
        Get average memory usage grouped by DAG.

        Args:
            session: Database session
            days: Number of days to look back

        Returns:
            List of dictionaries with dag_id and average metrics
        """
        from datetime import timedelta
        from sqlalchemy import func, cast, Float

        cutoff_date = datetime.utcnow() - timedelta(days=days)

        query = (
            select(
                cls.dag_id,
                func.avg(cls.peak_memory_mb).label("avg_peak_memory_mb"),
                func.avg(cls.memory_delta_mb).label("avg_memory_delta_mb"),
                func.count(cls.id).label("parse_count"),
            )
            .where(cls.parse_date >= cutoff_date)
            .group_by(cls.dag_id)
            .order_by(func.avg(cls.peak_memory_mb).desc())
        )

        result = session.execute(query)
        return [
            {
                "dag_id": row.dag_id,
                "avg_peak_memory_mb": row.avg_peak_memory_mb,
                "avg_memory_delta_mb": row.avg_memory_delta_mb,
                "parse_count": row.parse_count,
            }
            for row in result
        ]
