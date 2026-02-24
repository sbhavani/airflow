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
"""Unit tests for TaskStateSimulator."""

from __future__ import annotations

import pytest

from airflow.dag_testing.task_simulator import TaskStateSimulator, simulate_task_state
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import State


@pytest.fixture
def task():
    """Create a task for testing."""
    dag = DAG(dag_id="test_dag", start_date=None)
    with dag:
        return EmptyOperator(task_id="test_task")


@pytest.fixture
def simulator():
    """Create a TaskStateSimulator for testing."""
    return TaskStateSimulator()


class TestTaskStateSimulator:
    """Tests for TaskStateSimulator class."""

    def test_create_task_instance_queued(self, simulator, task):
        """Test creating a task instance in queued state."""
        ti = simulator.create_task_instance(task, state=State.QUEUED)
        assert ti.state == State.QUEUED
        assert ti.task_id == "test_task"

    def test_create_task_instance_running(self, simulator, task):
        """Test creating a task instance in running state."""
        ti = simulator.create_task_instance(task, state=State.RUNNING)
        assert ti.state == State.RUNNING

    def test_create_task_instance_success(self, simulator, task):
        """Test creating a task instance in success state."""
        ti = simulator.create_task_instance(task, state=State.SUCCESS)
        assert ti.state == State.SUCCESS

    def test_create_task_instance_failed(self, simulator, task):
        """Test creating a task instance in failed state."""
        ti = simulator.create_task_instance(task, state=State.FAILED)
        assert ti.state == State.FAILED

    def test_create_task_instance_retry(self, simulator, task):
        """Test creating a task instance in retry state."""
        ti = simulator.create_task_instance(task, state=State.RETRY)
        assert ti.state == State.RETRY

    def test_create_task_instance_skipped(self, simulator, task):
        """Test creating a task instance in skipped state."""
        ti = simulator.create_task_instance(task, state=State.SKIPPED)
        assert ti.state == State.SKIPPED

    def test_create_task_instance_up_for_reschedule(self, simulator, task):
        """Test creating a task instance in up_for_reschedule state."""
        ti = simulator.create_task_instance(task, state=State.UP_FOR_RESCHEDULE)
        assert ti.state == State.UP_FOR_RESCHEDULE

    def test_invalid_state(self, simulator, task):
        """Test that invalid state raises error."""
        with pytest.raises(ValueError, match="Invalid state"):
            simulator.create_task_instance(task, state="invalid_state")

    def test_transition_queued_to_running(self, simulator, task):
        """Test transition from queued to running."""
        ti = simulator.create_task_instance(task, state=State.QUEUED)
        simulator.transition(ti, State.RUNNING)
        assert ti.state == State.RUNNING

    def test_transition_running_to_success(self, simulator, task):
        """Test transition from running to success."""
        ti = simulator.create_task_instance(task, state=State.RUNNING)
        simulator.transition(ti, State.SUCCESS)
        assert ti.state == State.SUCCESS

    def test_transition_running_to_failed(self, simulator, task):
        """Test transition from running to failed."""
        ti = simulator.create_task_instance(task, state=State.RUNNING)
        simulator.transition(ti, State.FAILED)
        assert ti.state == State.FAILED

    def test_transition_running_to_retry(self, simulator, task):
        """Test transition from running to retry."""
        ti = simulator.create_task_instance(task, state=State.RUNNING)
        simulator.transition(ti, State.RETRY)
        assert ti.state == State.RETRY

    def test_simulate_retry(self, simulator, task):
        """Test retry simulation."""
        ti = simulator.create_task_instance(task, state=State.FAILED)
        retry_tis = simulator.simulate_retry(ti, max_retries=3)

        assert len(retry_tis) == 3
        assert retry_tis[0].state == State.QUEUED

    def test_get_state_history(self, simulator, task):
        """Test getting state history."""
        ti = simulator.create_task_instance(task, state=State.QUEUED)
        history = simulator.get_state_history(ti)
        assert len(history) > 0


class TestSimulateTaskState:
    """Tests for simulate_task_state convenience function."""

    def test_simulate_task_state(self, task):
        """Test convenience function."""
        ti = simulate_task_state(task, State.QUEUED)
        assert ti.state == State.QUEUED
        assert ti.task_id == "test_task"
