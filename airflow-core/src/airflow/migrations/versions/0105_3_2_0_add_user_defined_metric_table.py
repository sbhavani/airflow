#
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

"""
Add user_defined_metric table.

Revision ID: 5d7c91f8b3e2
Revises: e42d9fcd10d9
Create Date: 2026-02-24 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID, TIMESTAMP

# revision identifiers, used by Alembic.
revision = "5d7c91f8b3e2"
down_revision = "e42d9fcd10d9"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Create user_defined_metric table."""
    op.create_table(
        "user_defined_metric",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("metric_name", sa.String(250), nullable=False),
        sa.Column("metric_label", sa.String(250), nullable=True),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("aggregation", sa.String(50), server_default="sum", nullable=False),
        sa.Column("source", sa.String(100), server_default="operator", nullable=False),
        sa.Column("unit", sa.String(50), nullable=True),
        sa.Column("timestamp", TIMESTAMP(), nullable=False),
        sa.Column("task_state", sa.String(20), nullable=True),
        sa.Column("tags", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("user_defined_metric_pkey")),
        sa.UniqueConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            "metric_name",
            "metric_label",
            name="udm_unique_metric",
        ),
    )

    # Create indexes
    op.create_index("udm_dag_id_idx", "user_defined_metric", ["dag_id"])
    op.create_index("udm_task_id_idx", "user_defined_metric", ["task_id"])
    op.create_index("udm_run_id_idx", "user_defined_metric", ["run_id"])
    op.create_index("udm_metric_name_idx", "user_defined_metric", ["metric_name"])
    op.create_index("udm_metric_label_idx", "user_defined_metric", ["metric_label"])
    op.create_index(
        "udm_metric_aggregation_idx",
        "user_defined_metric",
        ["dag_id", "task_id", "metric_name", "aggregation"],
    )
    op.create_index("udm_timestamp_idx", "user_defined_metric", ["timestamp"])


def downgrade():
    """Drop user_defined_metric table."""
    op.drop_index("udm_timestamp_idx", table_name="user_defined_metric")
    op.drop_index("udm_metric_aggregation_idx", table_name="user_defined_metric")
    op.drop_index("udm_metric_label_idx", table_name="user_defined_metric")
    op.drop_index("udm_metric_name_idx", table_name="user_defined_metric")
    op.drop_index("udm_run_id_idx", table_name="user_defined_metric")
    op.drop_index("udm_task_id_idx", table_name="user_defined_metric")
    op.drop_index("udm_dag_id_idx", table_name="user_defined_metric")
    op.drop_table("user_defined_metric")
