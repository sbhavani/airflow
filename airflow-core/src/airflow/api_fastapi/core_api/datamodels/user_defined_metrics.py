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
from typing import Annotated, Iterable

from pydantic import BaseModel, Field

from airflow.models.user_defined_metric import AggregationFunction


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


class UserDefinedMetricCollectionResponse(BaseModel):
    """Response for a collection of user-defined metrics."""

    user_defined_metrics: Iterable[UserDefinedMetricResponse]
    total_entries: int


class UserDefinedMetricAggregationResponse(BaseModel):
    """Response for aggregated user-defined metrics."""

    metric_name: str
    aggregation: str
    value: float
    count: int
