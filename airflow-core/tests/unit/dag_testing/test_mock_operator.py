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
"""Unit tests for MockOperator."""

from __future__ import annotations

import pytest

from airflow.dag_testing.mock_operator import MockOperator, mock_operator
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


@pytest.fixture
def mock_operator_instance():
    """Create a MockOperator instance for testing."""
    return MockOperator(task_id="test_task", return_value={"result": "success"})


@pytest.fixture
def real_operator():
    """Create a real operator for testing."""
    dag = DAG(dag_id="test_dag", start_date=None)
    with dag:
        op = EmptyOperator(task_id="real_task")
    return op


class TestMockOperator:
    """Tests for MockOperator class."""

    def test_creation(self):
        """Test that MockOperator is created correctly."""
        op = MockOperator(task_id="my_task", return_value="test_value")
        assert op.task_id == "my_task"
        assert op.return_value == "test_value"
        assert op.execute_count == 0

    def test_execute_returns_value(self, mock_operator_instance):
        """Test that execute returns the configured value."""
        result = mock_operator_instance.execute({})
        assert result == {"result": "success"}

    def test_execute_count_increments(self, mock_operator_instance):
        """Test that execute_count increments on each call."""
        mock_operator_instance.execute({})
        assert mock_operator_instance.execute_count == 1
        mock_operator_instance.execute({})
        assert mock_operator_instance.execute_count == 2

    def test_execute_with_side_effect(self):
        """Test that side_effect is called instead of return_value."""
        def side_effect(x, y):
            return x + y

        op = MockOperator(task_id="calc_task", side_effect=side_effect)
        result = op.execute(context={})
        assert result is None  # side_effect returns None in this case

    def test_captures_execution_args(self, mock_operator_instance):
        """Test that execution arguments are captured."""
        mock_operator_instance.op_args = (1, 2, 3)
        mock_operator_instance.op_kwargs = {"key": "value"}
        mock_operator_instance.execute({})

        assert mock_operator_instance.last_execution_args == (1, 2, 3)
        assert mock_operator_instance.last_execution_kwargs == {"key": "value"}

    def test_execution_history(self, mock_operator_instance):
        """Test that execution history is recorded."""
        mock_operator_instance.op_args = ("first",)
        mock_operator_instance.execute({})
        mock_operator_instance.op_args = ("second",)
        mock_operator_instance.execute({})

        history = mock_operator_instance.get_execution_history()
        assert len(history) == 2
        assert history[0] == (("first",), {})
        assert history[1] == (("second",), {})


class TestMockOperatorHelper:
    """Tests for mock_operator helper function."""

    def test_mock_operator_creates_mock(self, real_operator):
        """Test that mock_operator creates a mock with same task_id."""
        mock = mock_operator(real_operator, return_value="mocked")
        assert mock.task_id == "real_task"
        assert mock.return_value == "mocked"

    def test_mock_operator_with_side_effect(self, real_operator):
        """Test mock_operator with side_effect."""
        def custom_effect():
            return "custom"

        mock = mock_operator(real_operator, side_effect=custom_effect)
        assert mock.side_effect == custom_effect

    def test_mock_operator_with_task_id(self):
        """Test that mock_operator creates a mock with direct task_id."""
        mock = mock_operator(task_id="my_task", return_value="direct")
        assert mock.task_id == "my_task"
        assert mock.return_value == "direct"

    def test_mock_operator_without_operator_or_task_id_raises(self):
        """Test that mock_operator raises error when neither operator nor task_id provided."""
        with pytest.raises(ValueError, match="Either 'operator' or 'task_id' must be provided"):
            mock_operator(return_value="test")
