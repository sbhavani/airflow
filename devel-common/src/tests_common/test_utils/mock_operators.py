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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Callable

import attr

from tests_common.test_utils.compat import BaseOperatorLink
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

try:
    from airflow.models.xcom import XComModel as XCom
    from airflow.sdk import BaseOperator
except ImportError:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.models.xcom import XCom  # type: ignore[no-redef]

# Import exceptions after BaseOperator to avoid import issues
try:
    from airflow.exceptions import AirflowException, AirflowSkipException  # type: ignore[attr-defined]
except ImportError:
    # Fallback for cases where Airflow exceptions aren't available

    class AirflowException(Exception):
        """Fallback AirflowException for testing environments."""

        pass

    class AirflowSkipException(Exception):
        """Fallback AirflowSkipException for testing environments."""

        pass


class MockOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields: Sequence[str] = ("arg1", "arg2")

    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context: Context):
        pass


class AirflowLink(BaseOperatorLink):
    """Operator Link for Apache Airflow Website."""

    name = "airflow"

    def get_link(self, operator, *, ti_key):
        return "https://airflow.apache.org"


class EmptyExtraLinkTestOperator(BaseOperator):
    """
    Empty test operator with extra link.

    Example of an Operator that has an extra operator link
    and will be overridden by the one defined in tests/plugins/test_plugin.py.
    """

    operator_extra_links = (AirflowLink(),)


class EmptyNoExtraLinkTestOperator(BaseOperator):
    """
    Empty test operator without extra operator link.

    Example of an operator that has no extra Operator link.
    An operator link would be added to this operator via Airflow plugin.
    """

    operator_extra_links = ()


@attr.s(auto_attribs=True)
class CustomBaseIndexOpLink(BaseOperatorLink):
    """Custom Operator Link for Google BigQuery Console."""

    index: int = attr.ib()

    @property
    def name(self) -> str:
        return f"BigQuery Console #{self.index + 1}"

    @property
    def xcom_key(self) -> str:
        return f"bigquery_{self.index + 1}"

    def get_link(self, operator, *, ti_key):
        if AIRFLOW_V_3_0_PLUS:
            search_queries = XCom.get_many(
                task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id, key="search_query"
            ).first()

            search_queries = XCom.deserialize_value(search_queries)
        else:
            search_queries = XCom.get_one(
                task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id, key="search_query"
            )

        if not search_queries:
            return None
        if len(search_queries) < self.index:
            return None
        search_query = search_queries[self.index]
        return f"https://console.cloud.google.com/bigquery?j={search_query}"


class CustomOpLink(BaseOperatorLink):
    """Custom Operator with Link for Google Custom Search."""

    name = "Google Custom"

    def get_link(self, operator, *, ti_key):
        if AIRFLOW_V_3_0_PLUS:
            search_query = XCom.get_many(
                task_ids=ti_key.task_id,
                dag_ids=ti_key.dag_id,
                run_id=ti_key.run_id,
                map_indexes=ti_key.map_index,
                key="search_query",
            ).first()
            search_query = XCom.deserialize_value(search_query)
        else:
            search_query = XCom.get_one(
                task_id=ti_key.task_id,
                dag_id=ti_key.dag_id,
                run_id=ti_key.run_id,
                map_index=ti_key.map_index,
                key="search_query",
            )
        if not search_query:
            return None
        return f"http://google.com/custom_base_link?search={search_query}"


class CustomOperator(BaseOperator):
    """Custom Operator for testing purposes."""

    template_fields = ["bash_command"]
    custom_operator_name = "@custom"

    @property
    def operator_extra_links(self):
        """Return operator extra links."""
        # For mapped operators
        if not hasattr(self, "bash_command"):
            # For mapped operators, we return CustomOpLink since each mapped instance
            # will get its own link during runtime
            return (CustomOpLink(),)
        # For non-mapped operators
        if isinstance(self.bash_command, str) or self.bash_command is None:
            return (CustomOpLink(),)
        # For operators with multiple commands
        return (CustomBaseIndexOpLink(i) for i, _ in enumerate(self.bash_command))

    def __init__(self, bash_command=None, **kwargs):
        super().__init__(**kwargs)
        self.bash_command = bash_command

    def execute(self, context: Context):
        self.log.info("Hello World!")
        context["task_instance"].xcom_push(key="search_query", value="dummy_value")


class GoogleLink(BaseOperatorLink):
    """Operator Link for Apache Airflow Website for Google."""

    name = "google"
    operators = [EmptyNoExtraLinkTestOperator, CustomOperator]

    def get_link(self, operator, *, ti_key):
        return "https://www.google.com"


class AirflowLink2(BaseOperatorLink):
    """Operator Link for Apache Airflow Website for 1.10.5."""

    name = "airflow"
    operators = [EmptyExtraLinkTestOperator, EmptyNoExtraLinkTestOperator]

    def get_link(self, operator, *, ti_key):
        return "https://airflow.apache.org/1.10.5/"


class GithubLink(BaseOperatorLink):
    """Operator Link for Apache Airflow GitHub."""

    name = "github"

    def get_link(self, operator, *, ti_key):
        return "https://github.com/apache/airflow"


# =============================================================================
# Comprehensive Mock Operators for DAG Testing
# =============================================================================


class MockSensorOperator(BaseOperator):
    """
    Mock sensor operator for testing.

    Allows controlling the poke result to simulate sensor behavior.
    """

    template_fields: Sequence[str] = ("poke_interval", "timeout")

    def __init__(
        self,
        *,
        poke_interval: float = 60.0,
        timeout: float = 60.0 * 60 * 24 * 7,
        poke_result: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.poke_result = poke_result

    def execute(self, context: Context):
        """Return the configured poke result."""
        return self.poke_result

    def poke(self, context: Context) -> bool:
        """Poke the sensor to check if condition is met."""
        return self.poke_result


class MockBranchOperator(BaseOperator):
    """
    Mock branch operator for testing.

    Allows controlling which branch to follow.
    """

    template_fields: Sequence[str] = ("branches",)

    def __init__(
        self,
        *,
        branches: str | list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.branches = branches

    def execute(self, context: Context):
        """Return the branch to follow."""
        if self.branches is None:
            return None
        if isinstance(self.branches, list):
            # Return first branch if list
            return self.branches[0] if self.branches else None
        return self.branches


class MockFailOperator(BaseOperator):
    """
    Mock operator that can be configured to fail.

    Useful for testing error handling and retries.
    """

    template_fields: Sequence[str] = ("fail_message",)

    def __init__(
        self,
        *,
        fail_message: str = "Task failed",
        fail_at_execute: bool = True,
        return_value: Any = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.fail_message = fail_message
        self.fail_at_execute = fail_at_execute
        self.return_value = return_value

    def execute(self, context: Context):
        """Execute the operator, optionally raising an exception."""
        if self.fail_at_execute:
            raise AirflowException(self.fail_message)
        return self.return_value


class MockPythonOperator(BaseOperator):
    """
    Mock Python operator for testing.

    Allows specifying a callable to execute.
    """

    template_fields: Sequence[str] = ("python_callable",)

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any] | None = None,
        return_value: Any = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.return_value = return_value

    def execute(self, context: Context):
        """Execute the python callable or return the configured return value."""
        if self.python_callable is not None:
            return self.python_callable(context)
        return self.return_value


class MockIncrementOperator(BaseOperator):
    """
    Mock operator that increments a counter in XCom.

    Useful for testing task dependencies and XCom.
    """

    template_fields: Sequence[str] = ("counter_key",)

    def __init__(
        self,
        *,
        counter_key: str = "counter",
        increment_by: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.counter_key = counter_key
        self.increment_by = increment_by

    def execute(self, context: Context):
        """Increment the counter and push to XCom."""
        current = context["ti"].xcom_pull(key=self.counter_key, default=0)
        new_value = current + self.increment_by
        context["ti"].xcom_push(key=self.counter_key, value=new_value)
        return new_value


class MockSucceedOperator(BaseOperator):
    """
    Mock operator that always succeeds.

    Simple operator for testing DAG structure and dependencies.
    """

    template_fields: Sequence[str] = ("return_value",)

    def __init__(self, *, return_value: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def execute(self, context: Context):
        """Return the configured return value."""
        return self.return_value


class MockSensorsListOperator(BaseOperator):
    """
    Mock operator that accepts a list of sensor results.

    Useful for testing multiple sensors.
    """

    template_fields: Sequence[str] = ("sensor_results",)

    def __init__(
        self,
        *,
        sensor_results: list[bool] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sensor_results = sensor_results or []

    def execute(self, context: Context):
        """Return the list of sensor results."""
        return self.sensor_results


class MockSKippableOperator(BaseOperator):
    """
    Mock operator that respects the skip mechanism.

    Useful for testing task skipping logic.
    """

    template_fields: Sequence[str] = ("should_skip", "return_value")

    def __init__(
        self,
        *,
        should_skip: bool = False,
        return_value: Any = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.should_skip = should_skip
        self.return_value = return_value

    def execute(self, context: Context):
        """Execute or skip based on configuration."""
        if self.should_skip:
            raise AirflowSkipException("Task skipped")
        return self.return_value
