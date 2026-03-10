/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Box, Button, Collapse, Text } from "@chakra-ui/react";
import { useState } from "react";

import { ErrorCategoryBadge } from "./ErrorCategoryBadge";
import { PossibleCausesList } from "./PossibleCausesList";
import { RemediationStepsList } from "./RemediationStepsList";

export type ErrorDiagnosticsData = {
  error_category: string;
  error_type: string;
  error_summary: string;
  error_message: string;
  possible_causes: Array<{
    id: string;
    description: string;
    likelihood: "high" | "medium" | "low";
  }>;
  remediation_steps: Array<{
    id: string;
    description: string;
    priority: number;
    documentation_link?: string;
  }>;
  context: Record<string, unknown>;
  timestamp: string;
};

type ErrorDiagnosticsPanelProps = {
  errorDiagnostics: ErrorDiagnosticsData | null;
  isLoading?: boolean;
  onRetry?: () => void;
  onClear?: () => void;
};

export const ErrorDiagnosticsPanel = ({
  errorDiagnostics,
  isLoading = false,
  onRetry,
  onClear,
}: ErrorDiagnosticsPanelProps) => {
  const [showFullError, setShowFullError] = useState(false);

  if (isLoading) {
    return (
      <Box p={4} borderRadius="md" borderWidth="1px" borderColor="gray.200">
        <Text>Loading error diagnostics...</Text>
      </Box>
    );
  }

  if (!errorDiagnostics) {
    return null;
  }

  return (
    <Box
      p={4}
      borderRadius="md"
      borderWidth="1px"
      borderColor="gray.200"
      bg="white"
      role="region"
      aria-label="Error Diagnostics"
    >
      <Box display="flex" alignItems="center" mb={4}>
        <ErrorCategoryBadge category={errorDiagnostics.error_category} size="medium" />
        <Text ml={2} fontWeight="bold" fontSize="lg">
          {errorDiagnostics.error_summary}
        </Text>
      </Box>

      <Box mb={4}>
        <Text fontWeight="semibold" mb={2}>
          What happened?
        </Text>
        <Text color="gray.600">{errorDiagnostics.error_summary}</Text>
      </Box>

      {errorDiagnostics.possible_causes && errorDiagnostics.possible_causes.length > 0 && (
        <Box mb={4}>
          <Text fontWeight="semibold" mb={2}>
            Possible causes:
          </Text>
          <PossibleCausesList causes={errorDiagnostics.possible_causes} />
        </Box>
      )}

      {errorDiagnostics.remediation_steps && errorDiagnostics.remediation_steps.length > 0 && (
        <Box mb={4}>
          <Text fontWeight="semibold" mb={2}>
            Suggested actions:
          </Text>
          <RemediationStepsList steps={errorDiagnostics.remediation_steps} />
        </Box>
      )}

      <Collapse in={showFullError}>
        <Box mt={4} p={3} bg="gray.50" borderRadius="md">
          <Text fontWeight="semibold" mb={2}>
            Full error message:
          </Text>
          <Text fontFamily="mono" fontSize="sm" whiteSpace="pre-wrap">
            {errorDiagnostics.error_message}
          </Text>
        </Box>
      </Collapse>

      <Box mt={4} display="flex" gap={2}>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowFullError(!showFullError)}
          aria-expanded={showFullError}
        >
          {showFullError ? "Hide Full Error" : "View Full Error"}
        </Button>
        {onRetry && (
          <Button size="sm" colorScheme="blue" onClick={onRetry}>
            Retry Task
          </Button>
        )}
        {onClear && (
          <Button size="sm" variant="outline" onClick={onClear}>
            Clear
          </Button>
        )}
      </Box>
    </Box>
  );
};
