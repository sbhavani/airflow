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
"""API datamodels for DAG memory metrics."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from pydantic import AliasPath, Field

from airflow.api_fastapi.core_api.base import BaseModel


class DAGParseMemoryMetricResponse(BaseModel):
    """DAG Parse Memory Metric serializer for responses."""

    id: int
    dag_id: str
    file_path: str
    parse_date: datetime
    threshold_mb: int
    peak_memory_mb: float
    memory_delta_mb: float
    threshold_exceeded: bool
    dag_display_name: str | None = Field(default=None, validation_alias=AliasPath("dag_model", "dag_display_name"))


class DAGParseMemoryMetricCollectionResponse(BaseModel):
    """DAG Parse Memory Metric collection serializer for responses."""

    dag_memory_metrics: Iterable[DAGParseMemoryMetricResponse]
    total_entries: int


class DAGMemorySummaryResponse(BaseModel):
    """DAG memory summary serializer for responses."""

    dag_id: str
    avg_peak_memory_mb: float
    avg_memory_delta_mb: float
    parse_count: int
    threshold_exceeded_count: int = 0


class DAGMemorySummaryCollectionResponse(BaseModel):
    """DAG memory summary collection serializer for responses."""

    dag_memory_summaries: Iterable[DAGMemorySummaryResponse]
    total_entries: int
