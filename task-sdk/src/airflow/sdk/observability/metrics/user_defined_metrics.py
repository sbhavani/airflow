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
"""User-defined metrics types and aggregation functions."""
from __future__ import annotations

from enum import Enum


class AggregationFunction(str, Enum):
    """Aggregation functions for user-defined metrics.

    These define how multiple metric values should be aggregated when queried.
    """

    SUM = "sum"
    """Sum all metric values."""

    AVG = "avg"
    """Calculate the average of metric values."""

    MIN = "min"
    """Get the minimum metric value."""

    MAX = "max"
    """Get the maximum metric value."""

    COUNT = "count"
    """Count the number of metric values."""

    LAST = "last"
    """Get the last recorded metric value."""


# Mapping from MetricType to default AggregationFunction
_METRIC_TYPE_TO_AGGREGATION: dict[str, AggregationFunction] = {
    "count": AggregationFunction.SUM,
    "gauge": AggregationFunction.LAST,
    "timing": AggregationFunction.AVG,
}


class MetricType(str, Enum):
    """Types of user-defined metrics with automatic aggregation.

    These provide convenient types that automatically select appropriate
    aggregation functions for common use cases.
    """

    COUNT = "count"
    """Counter metric - aggregated by SUM.

    Use for: Number of records processed, API calls made, errors encountered.
    Example: records_processed=1000, records_processed=500 -> total=1500
    """

    GAUGE = "gauge"
    """Gauge metric - stores LAST value.

    Use for: Current queue depth, active connections, memory usage.
    Example: queue_depth=10, queue_depth=15 -> current value=15
    """

    TIMING = "timing"
    """Timing metric - aggregated by AVG, MIN, MAX.

    Use for: Response times, processing duration, latency measurements.
    Example: response_time=100ms, response_time=200ms -> avg=150ms, min=100ms, max=200ms
    """

    @property
    def default_aggregation(self) -> AggregationFunction:
        """Get the default aggregation function for this metric type."""
        return _METRIC_TYPE_TO_AGGREGATION[self.value]
