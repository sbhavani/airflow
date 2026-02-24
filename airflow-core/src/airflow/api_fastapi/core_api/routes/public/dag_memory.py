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
"""API routes for DAG memory metrics."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated

from fastapi import Depends, Query
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_memory import (
    DAGMemorySummaryCollectionResponse,
    DAGMemorySummaryResponse,
    DAGParseMemoryMetricCollectionResponse,
    DAGParseMemoryMetricResponse,
)
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.dagparsememory import DagParseMemoryMetric

dag_memory_router = AirflowRouter(tags=["DAGMemory"])


@dag_memory_router.get(
    "/dagMemoryMetrics",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.WARNING))],
)
def list_dag_memory_metrics(
    session: SessionDep,
    dag_id: Annotated[str | None, Query()] = None,
    file_path: Annotated[str | None, Query()] = None,
    parse_date_gte: Annotated[datetime | None, Query()] = None,
    parse_date_lte: Annotated[datetime | None, Query()] = None,
    limit: QueryLimit = QueryLimit(100),
    offset: QueryOffset = QueryOffset(0),
) -> DAGParseMemoryMetricCollectionResponse:
    """
    Get a list of DAG memory metrics.

    This endpoint returns memory usage metrics captured during DAG parsing operations.
    Results can be filtered by DAG ID, file path, and date range.
    """
    query = select(DagParseMemoryMetric)

    if dag_id:
        query = query.where(DagParseMemoryMetric.dag_id == dag_id)
    if file_path:
        query = query.where(DagParseMemoryMetric.file_path == file_path)
    if parse_date_gte:
        query = query.where(DagParseMemoryMetric.parse_date >= parse_date_gte)
    if parse_date_lte:
        query = query.where(DagParseMemoryMetric.parse_date <= parse_date_lte)

    query = query.order_by(DagParseMemoryMetric.parse_date.desc())

    dag_memory_select, total_entries = paginated_select(
        statement=query,
        filters=[],
        offset=offset,
        limit=limit,
        session=session,
    )

    dag_memory_metrics = session.scalars(dag_memory_select)

    return DAGParseMemoryMetricCollectionResponse(
        dag_memory_metrics=dag_memory_metrics,
        total_entries=total_entries,
    )


@dag_memory_router.get(
    "/dagMemoryMetrics/summary",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.WARNING))],
)
def get_dag_memory_summary(
    session: SessionDep,
    days: Annotated[int, Query(default=30, ge=1, le=365)] = 30,
    limit: QueryLimit = QueryLimit(100),
    offset: QueryOffset = QueryOffset(0),
) -> DAGMemorySummaryCollectionResponse:
    """
    Get memory usage summary grouped by DAG.

    This endpoint returns aggregated memory usage statistics for each DAG,
    showing average memory consumption over the specified time period.
    """
    from sqlalchemy import func

    cutoff_date = datetime.utcnow() - timedelta(days=days)

    # Get summary by DAG with threshold exceeded count
    summary_query = (
        select(
            DagParseMemoryMetric.dag_id,
            func.avg(DagParseMemoryMetric.peak_memory_mb).label("avg_peak_memory_mb"),
            func.avg(DagParseMemoryMetric.memory_delta_mb).label("avg_memory_delta_mb"),
            func.count(DagParseMemoryMetric.id).label("parse_count"),
            func.sum(
                DagParseMemoryMetric.threshold_exceeded.cast(int)  # type: ignore[attr-defined]
            ).label("threshold_exceeded_count"),
        )
        .where(DagParseMemoryMetric.parse_date >= cutoff_date)
        .group_by(DagParseMemoryMetric.dag_id)
        .order_by(func.avg(DagParseMemoryMetric.peak_memory_mb).desc())
        .offset(offset.value)
        .limit(limit.value)
    )

    result = session.execute(summary_query)
    summaries = [
        DAGMemorySummaryResponse(
            dag_id=row.dag_id,
            avg_peak_memory_mb=float(row.avg_peak_memory_mb or 0),
            avg_memory_delta_mb=float(row.avg_memory_delta_mb or 0),
            parse_count=row.parse_count,
            threshold_exceeded_count=int(row.threshold_exceeded_count or 0),
        )
        for row in result
    ]

    # Get total count
    count_query = select(func.count(func.distinct(DagParseMemoryMetric.dag_id))).where(
        DagParseMemoryMetric.parse_date >= cutoff_date
    )
    total = session.scalar(count_query) or 0

    return DAGMemorySummaryCollectionResponse(
        dag_memory_summaries=summaries,
        total_entries=total,
    )


@dag_memory_router.get(
    "/dagMemoryMetrics/exceeded",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.WARNING))],
)
def get_dags_exceeding_threshold(
    session: SessionDep,
    days: Annotated[int, Query(default=30, ge=1, le=365)] = 30,
    limit: QueryLimit = QueryLimit(100),
    offset: QueryOffset = QueryOffset(0),
) -> DAGParseMemoryMetricCollectionResponse:
    """
    Get DAGs that have exceeded memory thresholds.

    This endpoint returns records of DAG parsing operations where the
    configured memory threshold was exceeded.
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days)

    query = (
        select(DagParseMemoryMetric)
        .where(DagParseMemoryMetric.threshold_exceeded == True)  # noqa: E712
        .where(DagParseMemoryMetric.parse_date >= cutoff_date)
        .order_by(DagParseMemoryMetric.peak_memory_mb.desc())
    )

    dag_memory_select, total_entries = paginated_select(
        statement=query,
        filters=[],
        offset=offset,
        limit=limit,
        session=session,
    )

    dag_memory_metrics = session.scalars(dag_memory_select)

    return DAGParseMemoryMetricCollectionResponse(
        dag_memory_metrics=dag_memory_metrics,
        total_entries=total_entries,
    )
