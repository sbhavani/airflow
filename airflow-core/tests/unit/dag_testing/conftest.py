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
"""Pytest configuration for dag_testing module.

This conftest exposes the fixtures from airflow.dag_testing.fixtures
so they can be used directly in tests.
"""

from __future__ import annotations

# Re-export fixtures from the main fixtures module
from airflow.dag_testing.fixtures import (
    dag_with_tasks,
    mock_operator_fixture,
    task_instance_factory,
    test_context,
    test_dag,
)

__all__ = [
    "test_dag",
    "mock_operator_fixture",
    "test_context",
    "task_instance_factory",
    "dag_with_tasks",
]
