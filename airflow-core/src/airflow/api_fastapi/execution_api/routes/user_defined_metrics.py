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
"""API routes for user-defined metrics."""
from __future__ import annotations

import json
import logging
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.user_defined_metric import (
    UserDefinedMetricBatchPayload,
    UserDefinedMetricBatchResponse,
    UserDefinedMetricPayload,
    UserDefinedMetricResponse,
)
from airflow.api_fastapi.execution_api.deps import JWTBearerTIPathDep
from airflow.models.user_defined_metric import UserDefinedMetric

log = logging.getLogger(__name__)

router = APIRouter(
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to emit metrics"},
        status.HTTP_404_NOT_FOUND: {"description": "Task instance not found"},
    },
    dependencies=[Depends(JWTBearerTIPathDep)],
)


@router.post(
    "/{dag_id}/{run_id}/{task_id}",
    status_code=status.HTTP_201_CREATED,
    description="Emit a user-defined metric for a task",
)
def emit_metric(
    dag_id: str,
    run_id: str,
    task_id: str,
    session: SessionDep,
    payload: UserDefinedMetricPayload,
    map_index: Annotated[int, Query(default=-1)] = -1,
) -> UserDefinedMetricResponse:
    """Emit a single user-defined metric."""
    from datetime import datetime, timezone

    metric = UserDefinedMetric(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        metric_name=payload.metric_name,
        metric_label=payload.metric_label,
        value=payload.value,
        aggregation=payload.aggregation.value,
        source="operator",
        unit=payload.unit,
        timestamp=datetime.now(timezone.utc),
        tags=json.dumps(payload.tags) if payload.tags else None,
    )
    session.add(metric)
    session.commit()
    session.refresh(metric)

    log.debug(
        "Emitted user-defined metric: dag_id=%s, task_id=%s, metric_name=%s, value=%s",
        dag_id,
        task_id,
        payload.metric_name,
        payload.value,
    )

    return UserDefinedMetricResponse(
        id=metric.id,
        dag_id=metric.dag_id,
        task_id=metric.task_id,
        run_id=metric.run_id,
        map_index=metric.map_index,
        metric_name=metric.metric_name,
        metric_label=metric.metric_label,
        value=metric.value,
        aggregation=metric.aggregation,
        source=metric.source,
        unit=metric.unit,
        timestamp=metric.timestamp,
        task_state=metric.task_state,
    )


@router.post(
    "/{dag_id}/{run_id}/{task_id}/batch",
    status_code=status.HTTP_201_CREATED,
    description="Emit multiple user-defined metrics for a task",
)
def emit_metrics_batch(
    dag_id: str,
    run_id: str,
    task_id: str,
    session: SessionDep,
    payload: UserDefinedMetricBatchPayload,
    map_index: Annotated[int, Query(default=-1)] = -1,
) -> UserDefinedMetricBatchResponse:
    """Emit multiple user-defined metrics at once."""
    from datetime import datetime, timezone

    metrics = []
    for metric_payload in payload.metrics:
        metric = UserDefinedMetric(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
            metric_name=metric_payload.metric_name,
            metric_label=metric_payload.metric_label,
            value=metric_payload.value,
            aggregation=metric_payload.aggregation.value,
            source="operator",
            unit=metric_payload.unit,
            timestamp=datetime.now(timezone.utc),
            tags=json.dumps(metric_payload.tags) if metric_payload.tags else None,
        )
        metrics.append(metric)

    session.add_all(metrics)
    session.commit()

    for metric in metrics:
        session.refresh(metric)

    log.debug(
        "Emitted %d user-defined metrics: dag_id=%s, task_id=%s",
        len(metrics),
        dag_id,
        task_id,
    )

    return UserDefinedMetricBatchResponse(
        count=len(metrics),
        ids=[m.id for m in metrics],
    )
