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
"""Example tests demonstrating all DAG testing utilities.

This file provides comprehensive examples of how to use the dag_testing utilities.
"""

from __future__ import annotations

from datetime import datetime

from airflow.dag_testing import (
    MockOperator,
    TaskStateSimulator,
    assert_no_circular_dependencies,
    assert_task_depends_on,
    assert_task_exists,
    assert_task_has_downstream,
    assert_task_naming_convention,
    get_dag_structure,
    mock_operator,
)
from airflow.dag_testing.serialization import validate_dag_serialization
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State


# Example 1: Using DAG Structure Validation
def test_dag_structure_validation():
    """Demonstrate DAG structure validation."""
    dag = DAG(dag_id="example_dag", start_date=datetime.now())
    with dag:
        task1 = EmptyOperator(task_id="start")
        task2 = EmptyOperator(task_id="process")
        task3 = EmptyOperator(task_id="end")

        task1 >> task2 >> task3

    # Verify task exists
    assert_task_exists(dag, "start")
    assert_task_exists(dag, "process")
    assert_task_exists(dag, "end")

    # Verify dependencies
    assert_task_depends_on(dag, "process", ["start"])
    assert_task_depends_on(dag, "end", ["process"])

    # Verify no circular dependencies
    assert_no_circular_dependencies(dag)

    # Validate serialization
    errors = validate_dag_serialization(dag)
    assert errors == [], f"Serialization errors: {errors}"

    print("DAG structure validation passed!")


# Example 2: Using Mock Operators
def test_mock_operator_usage():
    """Demonstrate mock operator usage."""
    dag = DAG(dag_id="mock_example", start_date=datetime.now())

    # Create a mock operator
    mock = MockOperator(
        task_id="api_call",
        return_value={"status": 200, "data": "test"},
    )

    # Execute the mock
    result = mock.execute({})

    assert result == {"status": 200, "data": "test"}
    assert mock.execute_count == 1

    # Verify execution was recorded
    history = mock.get_execution_history()
    assert len(history) == 1

    print("Mock operator test passed!")


# Example 3: Replacing Real Operators with Mocks
def test_mock_operator_replacement():
    """Demonstrate replacing real operators with mocks."""
    dag = DAG(dag_id="replacement_example", start_date=datetime.now())

    # Create a real operator
    with dag:
        real_task = PythonOperator(
            task_id="real_task",
            python_callable=lambda: "expensive_operation",
        )

    # Create a mock replacement
    mock_task = mock_operator(real_task, return_value="mocked_result")

    # Use the mock in the DAG
    dag.add_task(mock_task)

    # Execute
    result = mock_task.execute({})

    assert result == "mocked_result"
    assert mock_task.execute_count == 1

    print("Mock replacement test passed!")


# Example 4: Using Task Lifecycle Simulation
def test_task_lifecycle_simulation():
    """Demonstrate task lifecycle simulation."""
    simulator = TaskStateSimulator()

    dag = DAG(dag_id="lifecycle_example", start_date=datetime.now())
    with dag:
        task = EmptyOperator(task_id="test_task")

    # Create task in queued state
    ti = simulator.create_task_instance(task, state=State.QUEUED)
    assert ti.state == State.QUEUED

    # Transition to running
    simulator.transition(ti, State.RUNNING)
    assert ti.state == State.RUNNING

    # Transition to success
    simulator.transition(ti, State.SUCCESS)
    assert ti.state == State.SUCCESS

    print("Task lifecycle simulation passed!")


# Example 5: Using Fixtures
def test_using_fixtures(test_dag, mock_operator_fixture, test_context):
    """Demonstrate using fixtures."""
    # Use the test_dag fixture
    assert test_dag.dag_id == "test_dag"

    # Use the mock_operator_fixture
    result = mock_operator_fixture.execute({})
    assert result == {"status": "success"}

    # Use the test_context
    assert "execution_date" in test_context
    print(f"Execution date: {test_context['execution_date']}")

    print("Fixtures test passed!")


# Example 6: Using DAG Structure Helper
def test_get_dag_structure_helper():
    """Demonstrate getting DAG structure."""
    dag = DAG(dag_id="structure_example", start_date=datetime.now())
    with dag:
        start = EmptyOperator(task_id="start")
        middle = EmptyOperator(task_id="middle")
        end = EmptyOperator(task_id="end")

        start >> [middle, end]

    structure = get_dag_structure(dag)

    assert structure["dag_id"] == "structure_example"
    assert "start" in structure["tasks"]
    assert "middle" in structure["tasks"]
    assert "end" in structure["tasks"]
    assert set(structure["dependencies"]["end"]["upstream"]) == {"middle"}

    print("Get DAG structure test passed!")


# Example 7: Testing Naming Conventions
def test_task_naming_convention():
    """Demonstrate task naming convention validation."""
    dag = DAG(dag_id="naming_example", start_date=datetime.now())
    with dag:
        EmptyOperator(task_id="task_1")
        EmptyOperator(task_id="task_2")
        EmptyOperator(task_id="task_3")

    # Validate naming convention
    assert_task_naming_convention(dag, r"^task_\d+$")

    print("Naming convention test passed!")


# Run all examples
if __name__ == "__main__":
    test_dag_structure_validation()
    test_mock_operator_usage()
    test_mock_operator_replacement()
    test_task_lifecycle_simulation()
    test_get_dag_structure_helper()
    test_task_naming_convention()
    print("\nAll example tests passed!")
