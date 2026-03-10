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
"""DAG Testing Utilities.

This module provides testing utilities for Apache Airflow DAGs, including:
- Mock operators for replacing real operators in tests
- Task lifecycle simulation utilities
- DAG structure validation assertions
- Reusable pytest fixtures
"""

from __future__ import annotations

from airflow.dag_testing.mock_operator import MockOperator, mock_operator
from airflow.dag_testing.serialization import (
    get_serialization_errors,
    validate_dag_serialization,
)
from airflow.dag_testing.task_simulator import TaskStateSimulator
from airflow.dag_testing.validators import (
    assert_no_circular_dependencies,
    assert_task_depends_on,
    assert_task_exists,
    assert_task_has_downstream,
    assert_task_naming_convention,
    get_dag_structure,
)

__all__ = [
    "MockOperator",
    "mock_operator",
    "TaskStateSimulator",
    "assert_task_exists",
    "assert_task_depends_on",
    "assert_task_has_downstream",
    "assert_no_circular_dependencies",
    "assert_task_naming_convention",
    "get_dag_structure",
    "validate_dag_serialization",
    "get_serialization_errors",
]
