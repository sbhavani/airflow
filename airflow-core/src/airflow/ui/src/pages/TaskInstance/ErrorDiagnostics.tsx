/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
import { Box, Heading, HStack, List, ListItem, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdError, MdLightbulb, MdWarning } from "react-icons/md";

import { Alert } from "src/components/ui";

import type { ErrorDiagnosticResponse, TaskInstanceHistoryResponse } from "openapi/requests/types.gen";

type ErrorDiagnosticsProps = {
  readonly errorDiagnostic: ErrorDiagnosticResponse | null | undefined;
  readonly error: string | null | undefined;
};

export const ErrorDiagnostics = ({ errorDiagnostic, error }: ErrorDiagnosticsProps) => {
  const { t: translate } = useTranslation();

  // Only show error diagnostics for failed or retrying tasks
  if (!errorDiagnostic && !error) {
    return null;
  }

  return (
    <Box>
      <Heading size="md" mb={2}>
        {translate("taskInstance.errorDiagnostics")}
      </Heading>
      {error && (
        <Box mb={3}>
          <Text fontWeight="bold" mb={1}>
            {translate("taskInstance.errorMessage")}
          </Text>
          <Box
            bg="bg.subtle"
            p={2}
            borderRadius="md"
            fontFamily="mono"
            fontSize="sm"
            maxH="150px"
            overflowY="auto"
          >
            <Text whiteSpace="pre-wrap">{error}</Text>
          </Box>
        </Box>
      )}
      {errorDiagnostic && (
        <VStack align="start" gap={3}>
          <Alert status="warning" startElement={<MdWarning />}>
            <Box>
              <Text fontWeight="bold" mb={1}>
                {translate("taskInstance.errorType")}: {errorDiagnostic.error_type}
              </Text>
            </Box>
          </Alert>
          <Box>
            <HStack mb={1}>
              <MdError />
              <Text fontWeight="bold">{translate("taskInstance.possibleCauses")}</Text>
            </HStack>
            <List.Root as="ul" listStyleType="disc" pl={4}>
              {errorDiagnostic.possible_causes.map((cause, index) => (
                <ListItem key={index}>
                  <Text>{cause}</Text>
                </ListItem>
              ))}
            </List.Root>
          </Box>
          <Box>
            <HStack mb={1}>
              <MdLightbulb />
              <Text fontWeight="bold">{translate("taskInstance.remediationSteps")}</Text>
            </HStack>
            <List.Root as="ul" listStyleType="decimal" pl={4}>
              {errorDiagnostic.remediation_steps.map((step, index) => (
                <ListItem key={index}>
                  <Text>{step}</Text>
                </ListItem>
              ))}
            </List.Root>
          </Box>
        </VStack>
      )}
    </Box>
  );
};

type TryErrorDiagnosticsProps = {
  readonly tryInstance: TaskInstanceHistoryResponse | undefined;
};

export const TryErrorDiagnostics = ({ tryInstance }: TryErrorDiagnosticsProps) => {
  if (!tryInstance) {
    return null;
  }

  return (
    <ErrorDiagnostics
      error={tryInstance.error}
      errorDiagnostic={tryInstance.error_diagnostic}
    />
  );
};
