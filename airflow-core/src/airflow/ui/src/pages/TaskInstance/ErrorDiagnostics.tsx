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
import {
  Box,
  Button,
  Code,
  Heading,
  HStack,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiAlertCircle, FiCheckCircle, FiExternalLink, FiInfo } from "react-icons/fi";

// Error category to display name mapping
const errorCategoryNames: Record<string, string> = {
  connection: "Connection Error",
  authentication: "Authentication Error",
  permission: "Permission Error",
  resource: "Resource Error",
  timeout: "Timeout Error",
  validation: "Validation Error",
  external_service: "External Service Error",
  code_error: "Code Error",
  configuration: "Configuration Error",
  external_termination: "External Termination",
  unknown: "Unknown Error",
};

// Error category to documentation URL mapping
const errorCategoryDocs: Record<string, string | undefined> = {
  connection: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html",
  authentication: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/connections.html",
  permission: "https://airflow.apache.org/docs/apache-airflow/stable/security/index.html",
  resource: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/pools.html",
  timeout: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#timeouts",
  validation: undefined,
  external_service: undefined,
  code_error: undefined,
  configuration: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html",
  external_termination: undefined,
  unknown: undefined,
};

export type ErrorDiagnostic = {
  title: string;
  description: string;
  action: string;
  documentation_url?: string;
};

type ErrorDiagnosticsProps = {
  readonly error: string | undefined;
  readonly errorCategory: string | undefined;
};

const getCategoryDisplayName = (category: string | undefined): string => {
  if (!category) return "Unknown Error";
  return errorCategoryNames[category] ?? "Unknown Error";
};

const ErrorDiagnosticCard = ({ diagnostic }: { readonly diagnostic: ErrorDiagnostic }) => {
  const { t: translate } = useTranslation();

  return (
    <Box
      borderColor="border.subtle"
      borderRadius="md"
      borderWidth="1px"
      p={4}
      bg="bg.subtle"
    >
      <HStack mb={2}>
        <FiInfo color="var(--chakra-colors-blue-500)" />
        <Heading size="sm">{diagnostic.title}</Heading>
      </HStack>
      <Text color="fg.muted" mb={3} fontSize="sm">
        {diagnostic.description}
      </Text>
      <Box>
        <Text fontWeight="medium" mb={1} fontSize="sm">
          {translate("common:recommendedAction", "Recommended Action")}
        </Text>
        <Code
          display="block"
          whiteSpace="pre-wrap"
          p={2}
          borderRadius="md"
          fontSize="xs"
          bg="bg"
        >
          {diagnostic.action}
        </Code>
      </Box>
      {diagnostic.documentation_url && (
        <Button
          as="a"
          href={diagnostic.documentation_url}
          target="_blank"
          rel="noopener noreferrer"
          size="sm"
          mt={3}
          variant="ghost"
          rightIcon={<FiExternalLink />}
        >
          {translate("common:viewDocumentation", "View Documentation")}
        </Button>
      )}
    </Box>
  );
};

// Default diagnostics based on category
const getDefaultDiagnostics = (category: string | undefined): ErrorDiagnostic[] => {
  const categoryLower = category?.toLowerCase() ?? "";

  const defaultDiagnostics: Record<string, ErrorDiagnostic[]> = {
    timeout: [
      {
        title: "Operation Timeout",
        description: "The task operation took longer than the allowed time limit.",
        action: "1. Increase the timeout value in the operator\n2. Check if the remote service is experiencing slow response times\n3. Optimize the operation to complete faster\n4. Check for network latency issues",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#timeouts",
      },
    ],
    connection: [
      {
        title: "Connection Refused",
        description: "The task failed to connect to a remote service.",
        action: "1. Verify the remote service is running and accessible\n2. Check network connectivity and firewall rules\n3. Verify the connection string/hostname is correct\n4. Ensure the required port is open",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html",
      },
    ],
    authentication: [
      {
        title: "Authentication Failed",
        description: "The task failed to authenticate with a remote service.",
        action: "1. Verify the credentials are correct\n2. Check if the credentials have expired\n3. Ensure the user has necessary permissions\n4. Check for any required API keys or tokens",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/connections.html",
      },
    ],
    permission: [
      {
        title: "Permission Denied",
        description: "The task does not have the required permissions.",
        action: "1. Check the service account/role has necessary permissions\n2. Verify the IAM policies are correctly configured\n3. Ensure the user has access to the specific resource\n4. Review the resource ACLs",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/security/index.html",
      },
    ],
    validation: [
      {
        title: "Validation Error",
        description: "The task received invalid data that failed validation checks.",
        action: "1. Check the input data format and structure\n2. Verify all required fields are present\n3. Check for type mismatches in the data\n4. Review recent changes to upstream tasks",
      },
    ],
    resource: [
      {
        title: "Resource Exhausted",
        description: "The task ran out of system resources.",
        action: "1. Increase the pool slots allocated to the task\n2. Optimize the task to use less memory\n3. Check for memory leaks in the code\n4. Consider using a machine with more resources",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/pools.html",
      },
    ],
    external_service: [
      {
        title: "External Service Error",
        description: "An external API or service returned an error response.",
        action: "1. Check the status of the external service\n2. Review the API response for error details\n3. Implement retry logic for transient failures\n4. Check for rate limiting or quota issues",
      },
    ],
    code_error: [
      {
        title: "Code Execution Error",
        description: "The Python code in the task raised an exception.",
        action: "1. Check the task logs for the full traceback\n2. Review the exception type and message\n3. Check for typos or missing imports\n4. Verify the Python environment has required packages",
      },
    ],
    configuration: [
      {
        title: "Configuration Error",
        description: "The task failed due to missing or invalid configuration.",
        action: "1. Check the Airflow variables and connections\n2. Verify all required configuration keys are set\n3. Review the DAG or operator configuration\n4. Ensure all required secrets are properly configured",
        documentation_url: "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html",
      },
    ],
    external_termination: [
      {
        title: "Task Externally Terminated",
        description: "The task was terminated by an external process or signal.",
        action: "1. Check if the task was manually stopped\n2. Review system logs for termination signals\n3. Check for resource constraints that might have caused the termination\n4. Verify the task was not killed due to timeout",
      },
    ],
  };

  return defaultDiagnostics[categoryLower] ?? [
    {
      title: "Unknown Error",
      description: "An unexpected error occurred. Please check the task logs for more details.",
      action: "1. Review the full task logs for error details\n2. Check if this is a known issue with the operator\n3. Search for the error message online\n4. Consider adding custom error handling",
    },
  ];
};

export const ErrorDiagnostics = ({ error, errorCategory }: ErrorDiagnosticsProps) => {
  const { t: translate } = useTranslation();

  if (!error) {
    return null;
  }

  const diagnostics = getDefaultDiagnostics(errorCategory);
  const categoryDisplayName = getCategoryDisplayName(errorCategory);
  const docsUrl = errorCategory ? errorCategoryDocs[errorCategory.toLowerCase()] : undefined;

  return (
    <Box>
      <HStack mb={4}>
        <FiAlertCircle color="var(--chakra-colors-red-500)" />
        <Heading size="md">{translate("dag:errorDiagnostics", "Error Diagnostics")}</Heading>
      </HStack>

      <Box mb={4}>
        <HStack mb={2}>
          <Text fontWeight="medium" fontSize="sm">
            {translate("dag:errorDiagnosticsSection.errorCategory", "Error Category")}:
          </Text>
          <Text color="fg.muted" fontSize="sm">
            {categoryDisplayName}
          </Text>
        </HStack>

        {docsUrl && (
          <Button
            as="a"
            href={docsUrl}
            target="_blank"
            rel="noopener noreferrer"
            size="sm"
            variant="ghost"
            rightIcon={<FiExternalLink />}
            mb={3}
          >
            {translate("common:viewDocumentation", "View Documentation")}
          </Button>
        )}

        <Text fontWeight="medium" mb={1} fontSize="sm">
          {translate("dag:errorDiagnosticsSection.errorMessage", "Error Message")}:
        </Text>
        <Code
          display="block"
          whiteSpace="pre-wrap"
          p={3}
          borderRadius="md"
          fontSize="xs"
          bg="bg.subtle"
          maxH="200px"
          overflowY="auto"
        >
          {error}
        </Code>
      </Box>

      <Heading size="sm" mb={3}>
        {translate("dag:errorDiagnosticsSection.suggestedActions", "Suggested Actions")}
      </Heading>

      <VStack gap={3} align="stretch">
        {diagnostics.map((diagnostic, index) => (
          <ErrorDiagnosticCard key={index} diagnostic={diagnostic} />
        ))}
      </VStack>
    </Box>
  );
};
