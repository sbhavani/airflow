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
"""Data models for user-defined metrics API."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field

from airflow.models.user_defined_metric import AggregationFunction


class UserDefinedMetricPayload(BaseModel):
    """Payload for emitting a user-defined metric."""

    metric_name: Annotated[str, Field(min_length=1, max_length=250, description="Name of the metric")]
    metric_label: Annotated[
        str | None,
        Field(default=None, max_length=250, description="Optional label for the metric"),
    ]
    value: Annotated[float, Field(description="Numeric value of the metric")]
    aggregation: Annotated[
        AggregationFunction,
        Field(default=AggregationFunction.SUM, description="Aggregation function to use"),
    ]
    unit: Annotated[str | None, Field(default=None, max_length=50, description="Unit of the metric")]
    tags: Annotated[
        dict[str, str] | None,
        Field(default=None, description="Additional tags as key-value pairs"),
    ]


class UserDefinedMetricResponse(BaseModel):
    """Response for a user-defined metric."""

    id: int
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    metric_name: str
    metric_label: str | None
    value: float
    aggregation: str
    source: str
    unit: str | None
    timestamp: datetime
    task_state: str | None


class UserDefinedMetricBatchPayload(BaseModel):
    """Payload for emitting multiple user-defined metrics at once."""

    metrics: Annotated[
        list[UserDefinedMetricPayload],
        Field(min_length=1, max_length=100, description="List of metrics to emit"),
    ]


class UserDefinedMetricBatchResponse(BaseModel):
    """Response for batch metric emission."""

    count: int
    ids: list[int]
