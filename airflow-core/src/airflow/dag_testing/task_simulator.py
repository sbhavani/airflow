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
"""Task Lifecycle Simulation Utilities.

This module provides helpers for simulating task state transitions without running the scheduler.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.models import TaskInstance
from airflow.models.operator import Operator
from airflow.utils.state import State


# All valid Airflow task states
VALID_STATES: frozenset[str] = frozenset(
    {
        State.QUEUED,
        State.RUNNING,
        State.SUCCESS,
        State.FAILED,
        State.SKIPPED,
        State.RETRY,
        State.UP_FOR_RESCHEDULE,
        State.UP_FOR_RETRY,
        State.SCHEDULED,
        State.SENSING,
    }
)

# Valid state transitions
VALID_TRANSITIONS: dict[str, set[str]] = {
    State.QUEUED: {State.RUNNING, State.SKIPPED},
    State.RUNNING: {State.SUCCESS, State.FAILED, State.SKIPPED, State.RETRY, State.UP_FOR_RETRY},
    State.SUCCESS: set(),  # Terminal state
    State.FAILED: {State.RETRY, State.UP_FOR_RETRY},  # Can retry
    State.SKIPPED: set(),  # Terminal state
    State.RETRY: {State.QUEUED, State.RUNNING},  # Re-queue for retry
    State.UP_FOR_RESCHEDULE: {State.QUEUED, State.RUNNING},
    State.UP_FOR_RETRY: {State.RUNNING, State.QUEUED},
    State.SCHEDULED: {State.QUEUED, State.RUNNING},
    State.SENSING: {State.SUCCESS, State.FAILED},
}


class TaskStateSimulator:
    """Helper class for simulating task state transitions.

    This class allows creating task instances with specific states and
    transitioning them through the Airflow lifecycle without executing
    the actual operator logic.
    """

    def __init__(self) -> None:
        self._task_instances: list[TaskInstance] = []
        self._state_history: dict[int, list[str]] = {}  # Maps id(ti) to state history

    def create_task_instance(
        self,
        task: Operator,
        state: str = State.QUEUED,
        execution_date: datetime | None = None,
        **kwargs: Any,
    ) -> TaskInstance:
        """Create a TaskInstance with a specified state.

        :param task: The task to create an instance for
        :param state: Initial state (default: queued)
        :param execution_date: Execution date for the task instance
        :param kwargs: Additional arguments for TaskInstance

        :returns: A TaskInstance with the specified state

        :raises ValueError: If the state is not valid
        """
        if state not in VALID_STATES:
            raise ValueError(
                f"Invalid state: {state}. Valid states are: {sorted(VALID_STATES)}"
            )

        if execution_date is None:
            execution_date = datetime.now()

        # Create a minimal TaskInstance
        ti = TaskInstance(task=task, run_id=f"test_run_{self._task_instances}")
        ti.state = state
        ti.execution_date = execution_date

        # Store any additional kwargs
        for key, value in kwargs.items():
            if hasattr(ti, key):
                setattr(ti, key, value)

        self._task_instances.append(ti)
        # Initialize state history for this task instance
        self._state_history[id(ti)] = [state] if state else []
        return ti

    def transition(self, task_instance: TaskInstance, new_state: str) -> None:
        """Transition a TaskInstance to a new state.

        :param task_instance: The task instance to transition
        :param new_state: The new state to transition to

        :raises ValueError: If the state is not valid
        :raises AssertionError: If the transition is not valid
        """
        if new_state not in VALID_STATES:
            raise ValueError(
                f"Invalid state: {new_state}. Valid states are: {sorted(VALID_STATES)}"
            )

        current_state = task_instance.state

        # Allow None -> any state for initial creation
        if current_state is not None and current_state not in VALID_STATES:
            raise ValueError(f"Invalid current state: {current_state}")

        # Check if transition is valid
        valid_targets = VALID_TRANSITIONS.get(current_state, set())
        if current_state is not None and new_state not in valid_targets:
            # Some transitions are always allowed in tests
            if current_state in {None, State.QUEUED, State.SCHEDULED}:
                pass  # Allow initial transitions
            else:
                raise AssertionError(
                    f"Invalid state transition from {current_state} to {new_state}. "
                    f"Valid transitions from {current_state}: {valid_targets}"
                )

        task_instance.state = new_state

        # Record state transition in history
        ti_id = id(task_instance)
        if ti_id not in self._state_history:
            self._state_history[ti_id] = []
        self._state_history[ti_id].append(new_state)

    def simulate_retry(
        self, task_instance: TaskInstance, max_retries: int = 3
    ) -> list[TaskInstance]:
        """Simulate a complete retry cycle.

        :param task_instance: The task instance to retry
        :param max_retries: Maximum number of retries to simulate

        :returns: List of task instances created during retry simulation
        """
        retry_instances: list[TaskInstance] = []

        # First, set the task to failed
        task_instance.state = State.FAILED

        # Simulate retries
        for i in range(max_retries):
            # Create a new TI for the retry
            retry_ti = self.create_task_instance(
                task_instance.task,
                state=State.QUEUED,
                try_number=i + 2,  # Try numbers start at 1, retries are try_number > 1
            )
            retry_instances.append(retry_ti)

            # Transition through retry states
            self.transition(retry_ti, State.UP_FOR_RETRY)
            self.transition(retry_ti, State.QUEUED)
            self.transition(retry_ti, State.RUNNING)

        return retry_instances

    def get_state_history(self, task_instance: TaskInstance) -> list[str]:
        """Get the history of state transitions for a task instance.

        :param task_instance: The task instance to get history for

        :returns: List of states in transition order
        """
        ti_id = id(task_instance)
        return self._state_history.get(ti_id, [])


# Module-level convenience function for backward compatibility
def simulate_task_state(task: Operator, state: str) -> TaskInstance:
    """Simulate a task with a specific state.

    This is a convenience function that creates a TaskInstance with the specified state.

    :param task: The task to simulate
    :param state: The state to set

    :returns: A TaskInstance with the specified state
    """
    simulator = TaskStateSimulator()
    return simulator.create_task_instance(task, state)
