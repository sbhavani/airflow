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

"""Remediation content loader for error diagnostics."""

from __future__ import annotations

import os
from typing import Any

import yaml


# Cache for loaded remediation content
_REMEDIATION_CACHE: dict[str, dict[str, Any]] = {}


def get_remediation_for_error(category: str, error_type: str) -> dict[str, Any]:
    """
    Get remediation content for a specific error.

    Args:
        category: The error category (e.g., CONNECTION, AUTHENTICATION)
        error_type: The specific error type

    Returns:
        Dictionary containing possible causes and remediation steps
    """
    cache_key = f"{category}:{error_type}"

    if cache_key in _REMEDIATION_CACHE:
        return _REMEDIATION_CACHE[cache_key]

    # Try to load from config file
    remediation = _load_from_config(category, error_type)

    if remediation:
        _REMEDIATION_CACHE[cache_key] = remediation
        return remediation

    # Return generic fallback
    return _get_generic_remediation(category, error_type)


def _load_from_config(category: str, error_type: str) -> dict[str, Any] | None:
    """Load remediation content from YAML config file."""
    # Get the config directory path
    config_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "error_diagnostics",
    )

    config_file = os.path.join(config_dir, f"{category.lower()}.yaml")

    if not os.path.exists(config_file):
        return None

    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        if not config:
            return None

        # Find the remediation for this specific error type
        remediations = config.get("remediations", [])
        for rem in remediations:
            if rem.get("error_type") == error_type:
                return rem

        return None
    except Exception:
        return None


def _get_generic_remediation(category: str, error_type: str) -> dict[str, Any]:
    """Get generic fallback remediation when no specific config exists."""
    base_url = "https://airflow.apache.org/docs/apache-airflow/stable"

    generic_remediations = {
        "CONNECTION": {
            "possible_causes": [
                {"id": "network_issue", "description": "Network connectivity issue", "likelihood": "high"},
                {"id": "service_down", "description": "External service is unavailable", "likelihood": "medium"},
                {"id": "firewall", "description": "Firewall or network policy blocking connection", "likelihood": "low"},
            ],
            "remediation_steps": [
                {
                    "id": "check_network",
                    "description": "Check network connectivity to the target service",
                    "priority": 1,
                    "documentation_link": f"{base_url}/troubleshooting.html#network-connectivity",
                },
                {
                    "id": "verify_service",
                    "description": "Verify the external service is running and accessible",
                    "priority": 2,
                    "documentation_link": f"{base_url}/troubleshooting.html#external-services",
                },
            ],
        },
        "AUTHENTICATION": {
            "possible_causes": [
                {"id": "invalid_cred", "description": "Invalid credentials", "likelihood": "high"},
                {"id": "expired_cred", "description": "Credentials have expired", "likelihood": "medium"},
                {"id": "insufficient_perms", "description": "Insufficient permissions", "likelihood": "medium"},
            ],
            "remediation_steps": [
                {
                    "id": "verify_cred",
                    "description": "Verify credentials are correct and up to date",
                    "priority": 1,
                    "documentation_link": f"{base_url}/howto/connection.html",
                },
                {
                    "id": "check_perms",
                    "description": "Check that the service account has required permissions",
                    "priority": 2,
                    "documentation_link": f"{base_url}/security.html",
                },
            ],
        },
        "TIMEOUT": {
            "possible_causes": [
                {"id": "slow_service", "description": "Service is responding slowly", "likelihood": "high"},
                {"id": "large_data", "description": "Processing large amount of data", "likelihood": "medium"},
                {"id": "resource_constrained", "description": "System resources constrained", "likelihood": "low"},
            ],
            "remediation_steps": [
                {
                    "id": "increase_timeout",
                    "description": "Increase the timeout value for the task",
                    "priority": 1,
                    "documentation_link": f"{base_url}/core-concepts/tasks.html#timeouts",
                },
                {
                    "id": "optimize",
                    "description": "Optimize the task to process data faster",
                    "priority": 2,
                },
            ],
        },
        "UPSTREAM": {
            "possible_causes": [
                {"id": "upstream_failed", "description": "Upstream task failed", "likelihood": "high"},
                {"id": "trigger_rule", "description": "Trigger rule not met", "likelihood": "medium"},
            ],
            "remediation_steps": [
                {
                    "id": "fix_upstream",
                    "description": "Fix the failed upstream task and rerun",
                    "priority": 1,
                    "documentation_link": f"{base_url}/core-concepts/tasks.html#trigger-rules",
                },
                {
                    "id": "check_trigger",
                    "description": "Review trigger rules for this task",
                    "priority": 2,
                },
            ],
        },
    }

    return generic_remediations.get(category, {
        "possible_causes": [
            {"id": "unknown", "description": "Unknown error cause", "likelihood": "medium"},
        ],
        "remediation_steps": [
            {
                "id": "check_logs",
                "description": "Check task logs for more details",
                "priority": 1,
                "documentation_link": f"{base_url}/ui.html#task-logs",
            },
        ],
    })


def clear_cache():
    """Clear the remediation content cache."""
    global _REMEDIATION_CACHE
    _REMEDIATION_CACHE = {}
