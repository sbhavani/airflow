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
from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import (
    AliasPath,
    BeforeValidator,
    Field,
    model_validator,
)

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.api_fastapi.core_api.datamodels.task_instances import ErrorDiagnosticResponse
from airflow.utils.state import TaskInstanceState


class TaskInstanceHistoryResponse(BaseModel):
    """TaskInstanceHistory serializer for responses."""

    task_id: str
    dag_id: str

    # todo: this should not be aliased; it's ambiguous with dag run's "id" - airflow 3.0
    run_id: str = Field(alias="dag_run_id")

    map_index: int
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    state: TaskInstanceState | None
    try_number: int
    max_tries: int
    task_display_name: str
    dag_display_name: str = Field(validation_alias=AliasPath("dag_run", "dag_model", "dag_display_name"))
    hostname: str | None
    unixname: str | None
    pool: str
    pool_slots: int
    queue: str | None
    priority_weight: int | None
    operator: str | None
    custom_operator_name: str | None = Field(alias="operator_name")
    queued_dttm: datetime | None = Field(alias="queued_when")
    scheduled_dttm: datetime | None = Field(alias="scheduled_when")
    pid: int | None
    executor: str | None
    executor_config: Annotated[str, BeforeValidator(str)]
    dag_version: DagVersionResponse | None
    # Error message from the task execution attempt
    error: str | None = None
    # Error diagnostic with possible causes and remediation steps
    error_diagnostic: ErrorDiagnosticResponse | None = None

    @model_validator(mode="after")
    def compute_error_diagnostic(self) -> TaskInstanceHistoryResponse:
        """Compute error diagnostic from error message if available."""
        from airflow import error_diagnostics

        if self.error and self.state in (TaskInstanceState.FAILED, TaskInstanceState.UP_FOR_RETRY):
            # Try to extract error type from the error message
            error_msg = self.error
            error_type = None

            # Try to identify the error type from common Airflow exceptions
            for known_type in error_diagnostics.ERROR_DIAGNOSTICS:
                if known_type in error_msg:
                    error_type = known_type
                    break

            # If we found a matching error type, get the diagnostic
            if error_type:
                diagnostic = error_diagnostics.get_error_diagnostic(error_type)
                if diagnostic:
                    self.error_diagnostic = ErrorDiagnosticResponse(
                        error_type=diagnostic.error_type,
                        possible_causes=list(diagnostic.possible_causes),
                        remediation_steps=list(diagnostic.remediation_steps),
                    )
            else:
                # If we can't identify the error type, provide generic diagnostic
                self.error_diagnostic = ErrorDiagnosticResponse(
                    error_type="UnknownError",
                    possible_causes=[
                        "An unexpected error occurred during task execution",
                        "Check the task logs for more details",
                    ],
                    remediation_steps=[
                        "Review the task logs for specific error details",
                        "Check the task configuration and dependencies",
                        "Verify the external systems are accessible",
                    ],
                )

        return self


class TaskInstanceHistoryCollectionResponse(BaseModel):
    """TaskInstanceHistory Collection serializer for responses."""

    task_instances: list[TaskInstanceHistoryResponse]
    total_entries: int
