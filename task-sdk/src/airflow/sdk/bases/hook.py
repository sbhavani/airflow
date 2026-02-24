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

import logging
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from airflow.sdk.definitions._internal.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.sdk.definitions.connection import Connection

log = logging.getLogger(__name__)

# TypeVar for the connection type that a hook uses
T = TypeVar("T")


class BaseHook(LoggingMixin, Generic[T]):
    """
    Abstract base class for hooks.

    Hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.

    :param logger_name: Name of the logger used by the Hook to emit logs.
        If set to `None` (default), the logger name will fall back to
        `airflow.task.hooks.{class.__module__}.{class.__name__}` (e.g. DbApiHook will have
        *airflow.task.hooks.airflow.providers.common.sql.hooks.sql.DbApiHook* as logger).
    """

    conn_name_attr: str | None = None
    default_conn_name: str | None = None

    def __init__(self, logger_name: str | None = None) -> None:
        super().__init__()
        self._log_config_logger_name = "airflow.task.hooks"
        self._logger_name = logger_name
        self._conn: T | None = None

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        """
        Get connection, given connection id.

        :param conn_id: connection id
        :return: connection
        """
        from airflow.sdk.definitions.connection import Connection

        conn = Connection.get(conn_id)
        log.debug("Connection Retrieved '%s' (via task-sdk)", conn.conn_id)
        return conn

    @classmethod
    async def aget_connection(cls, conn_id: str) -> Connection:
        """
        Get connection (async), given connection id.

        :param conn_id: connection id
        :return: connection
        """
        from airflow.sdk.definitions.connection import Connection

        conn = await Connection.async_get(conn_id)
        log.debug("Connection Retrieved '%s' (via task-sdk)", conn.conn_id)
        return conn

    @classmethod
    def get_hook(cls, conn_id: str | None = None, hook_params: dict[str, Any] | None = None) -> BaseHook:
        """
        Return default hook for this connection id.

        :param conn_id: connection id
        :param hook_params: hook parameters
        :return: default hook for this connection
        """
        connection = cls.get_connection(conn_id or cls.default_conn_name or "")
        return connection.get_hook(hook_params=hook_params)

    def get_conn(self) -> T:
        """Return connection for the hook."""
        raise NotImplementedError()

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {}

    def close_conn(self) -> None:
        """Close the connection. Override in subclasses if needed."""
        self._conn = None

    def __enter__(self) -> BaseHook[T]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close_conn()
