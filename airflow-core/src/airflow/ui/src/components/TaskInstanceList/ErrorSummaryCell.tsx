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

import { Box, HStack } from "@chakra-ui/react";

import { ErrorCategoryBadge } from "src/components/ErrorDiagnostics/ErrorCategoryBadge";

import { TruncatedText } from "../TruncatedText";

export type ErrorDiagnosticsSummary = {
  error_category: string;
  error_summary: string;
};

type ErrorSummaryCellProps = {
  errorDiagnostics: ErrorDiagnosticsSummary | null | undefined;
};

export const ErrorSummaryCell = ({ errorDiagnostics }: ErrorSummaryCellProps) => {
  // Handle null or undefined error diagnostics gracefully
  if (!errorDiagnostics) {
    return null;
  }

  const { error_category: errorCategory, error_summary: errorSummary } = errorDiagnostics;

  // Don't render if there's no meaningful data
  if (!errorCategory && !errorSummary) {
    return null;
  }

  return (
    <HStack spacing={2} maxW="300px">
      {errorCategory && <ErrorCategoryBadge category={errorCategory} size="sm" />}
      {errorSummary && (
        <Box flex={1} minW={0}>
          <TruncatedText text={errorSummary} />
        </Box>
      )}
    </HStack>
  );
};
