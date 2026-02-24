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
"""Routes for user-defined metrics."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import case, func, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.user_defined_metrics import (
    UserDefinedMetricAggregationResponse,
    UserDefinedMetricCollectionResponse,
    UserDefinedMetricResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    DagAccessEntity,
    requires_access_dag,
)
from airflow.models.user_defined_metric import AggregationFunction, UserDefinedMetric

if TYPE_CHECKING:
    from sqlalchemy import Result

user_defined_metrics_router = AirflowRouter(tags=["User Defined Metrics"], prefix="/userDefinedMetrics")


@user_defined_metrics_router.get(
    "",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_user_defined_metrics(
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
    dag_id: Annotated[str | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    run_id: Annotated[str | None, Query()] = None,
    metric_name: Annotated[str | None, Query()] = None,
    metric_label: Annotated[str | None, Query()] = None,
    start_date: Annotated[datetime | None, Query()] = None,
    end_date: Annotated[datetime | None, Query()] = None,
) -> UserDefinedMetricCollectionResponse:
    """Get all user-defined metrics with optional filters."""
    query = select(UserDefinedMetric)

    # Apply filters
    if dag_id is not None:
        query = query.where(UserDefinedMetric.dag_id == dag_id)
    if task_id is not None:
        query = query.where(UserDefinedMetric.task_id == task_id)
    if run_id is not None:
        query = query.where(UserDefinedMetric.run_id == run_id)
    if metric_name is not None:
        query = query.where(UserDefinedMetric.metric_name == metric_name)
    if metric_label is not None:
        query = query.where(UserDefinedMetric.metric_label == metric_label)
    if start_date is not None:
        query = query.where(UserDefinedMetric.timestamp >= start_date)
    if end_date is not None:
        query = query.where(UserDefinedMetric.timestamp <= end_date)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = session.scalar(count_query) or 0

    # Apply pagination
    query = query.offset(offset.value).limit(limit.value)

    # Execute query
    result: Result[UserDefinedMetric] = session.execute(query)
    metrics = result.scalars().all()

    return UserDefinedMetricCollectionResponse(
        user_defined_metrics=[UserDefinedMetricResponse.model_validate(m) for m in metrics],
        total_entries=total,
    )


@user_defined_metrics_router.get(
    "/aggregations",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_user_defined_metric_aggregations(
    session: SessionDep,
    metric_name: Annotated[str, Query(description="Name of the metric to aggregate")],
    dag_id: Annotated[str | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    aggregation: Annotated[str, Query(description="Aggregation function: sum, avg, min, max, count, last")] = "sum",
    start_date: Annotated[datetime | None, Query()] = None,
    end_date: Annotated[datetime | None, Query()] = None,
) -> list[UserDefinedMetricAggregationResponse]:
    """Get aggregated values for a metric."""
    # Validate aggregation function
    valid_aggregations = {"sum", "avg", "min", "max", "count", "last"}
    if aggregation.lower() not in valid_aggregations:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Invalid aggregation function. Must be one of: {', '.join(valid_aggregations)}",
        )

    query = select(UserDefinedMetric).where(UserDefinedMetric.metric_name == metric_name)

    # Apply filters
    if dag_id is not None:
        query = query.where(UserDefinedMetric.dag_id == dag_id)
    if task_id is not None:
        query = query.where(UserDefinedMetric.task_id == task_id)
    if start_date is not None:
        query = query.where(UserDefinedMetric.timestamp >= start_date)
    if end_date is not None:
        query = query.where(UserDefinedMetric.timestamp <= end_date)

    # Execute query to get all matching metrics
    result: Result[UserDefinedMetric] = session.execute(query)
    metrics = result.scalars().all()

    if not metrics:
        return [
            UserDefinedMetricAggregationResponse(
                metric_name=metric_name,
                aggregation=aggregation,
                value=0.0,
                count=0,
            )
        ]

    # Calculate aggregation
    values = [m.value for m in metrics]
    agg_value: float
    agg_func = aggregation.lower()

    if agg_func == "sum":
        agg_value = sum(values)
    elif agg_func == "avg":
        agg_value = sum(values) / len(values)
    elif agg_func == "min":
        agg_value = min(values)
    elif agg_func == "max":
        agg_value = max(values)
    elif agg_func == "count":
        agg_value = len(values)
    elif agg_func == "last":
        # Get the most recent value
        sorted_metrics = sorted(metrics, key=lambda m: m.timestamp, reverse=True)
        agg_value = sorted_metrics[0].value if sorted_metrics else 0.0

    return [
        UserDefinedMetricAggregationResponse(
            metric_name=metric_name,
            aggregation=aggregation,
            value=agg_value,
            count=len(values),
        )
    ]
