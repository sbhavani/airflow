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
"""Mock Operator Utilities.

This module provides mock operators for testing DAGs without executing real operators.
"""

from __future__ import annotations

from typing import Any, Callable

from airflow.models import BaseOperator
from airflow.models.operator import Operator


class MockOperator(BaseOperator):
    """A configurable mock operator that can replace real operators in tests.

    This operator captures execution calls and their parameters for assertions,
    and supports configurable return values and side effects.

    :param task_id: Unique identifier for the task
    :param return_value: Value to return when executed
    :param side_effect: Optional callable to execute instead of returning value
    :param op_args: Positional arguments passed to execute
    :param op_kwargs: Keyword arguments passed to execute
    """

    template_fields: tuple[str, ...] = ()
    template_ext: tuple[str, ...] = ()
    ui_color: str = "#e4f0f8"

    def __init__(
        self,
        *,
        task_id: str,
        return_value: Any = None,
        side_effect: Callable[..., Any] | None = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.return_value = return_value
        self.side_effect = side_effect
        self.execute_count: int = 0
        self.last_execution_args: tuple[Any, ...] = ()
        self.last_execution_kwargs: dict[str, Any] = {}
        self.execution_history: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def execute(self, context: dict[str, Any]) -> Any:
        """Execute the mock operator.

        :param context: Airflow context dictionary

        :returns: The configured return_value or result of side_effect
        """
        self.execute_count += 1
        self.last_execution_args = self.op_args
        self.last_execution_kwargs = self.op_kwargs
        self.execution_history.append((self.op_args, self.op_kwargs))

        if self.side_effect is not None:
            return self.side_effect(*self.op_args, **self.op_kwargs)
        return self.return_value

    def get_execution_count(self) -> int:
        """Get the number of times execute was called.

        :returns: Number of executions
        """
        return self.execute_count

    def get_last_execution_args(self) -> tuple[Any, ...]:
        """Get the arguments from the last execution.

        :returns: Tuple of positional arguments
        """
        return self.last_execution_args

    def get_last_execution_kwargs(self) -> dict[str, Any]:
        """Get the keyword arguments from the last execution.

        :returns: Dictionary of keyword arguments
        """
        return self.last_execution_kwargs

    def get_execution_history(self) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        """Get the full execution history.

        :returns: List of (args, kwargs) tuples for each execution
        """
        return self.execution_history


def mock_operator(
    operator: Operator | None = None,
    return_value: Any = None,
    side_effect: Callable[..., Any] | None = None,
    *,
    task_id: str | None = None,
) -> MockOperator:
    """Create a MockOperator for testing.

    This function creates a mock operator that can be used as a drop-in replacement
    in tests. It supports two usage patterns:

    1. From an existing operator: mock_operator(real_operator, return_value="mocked")
    2. Direct creation: mock_operator(task_id="my_task", return_value="mocked")

    :param operator: The operator to mimic (optional if task_id is provided)
    :param return_value: Value to return when executed
    :param side_effect: Optional callable to execute instead of returning value
    :param task_id: Optional task_id for direct creation (alternative to operator)

    :returns: A configured MockOperator instance
    """
    if operator is not None:
        mock_task_id = operator.task_id
    elif task_id is not None:
        mock_task_id = task_id
    else:
        raise ValueError("Either 'operator' or 'task_id' must be provided")

    return MockOperator(
        task_id=mock_task_id,
        return_value=return_value,
        side_effect=side_effect,
    )
