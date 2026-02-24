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

import { Box, HStack, Link, Text } from "@chakra-ui/react";
import { FiExternalLink } from "react-icons/fi";

type RemediationStep = {
  id: string;
  description: string;
  priority: number;
  documentation_link?: string;
};

type RemediationStepsListProps = {
  steps: RemediationStep[];
};

export const RemediationStepsList = ({ steps }: RemediationStepsListProps) => {
  // Sort by priority
  const sortedSteps = [...steps].sort((a, b) => a.priority - b.priority);

  return (
    <Box as="ol" listStyleType="none" pl={0}>
      {sortedSteps.map((step, index) => (
        <HStack
          as="li"
          key={step.id}
          mb={3}
          alignItems="flex-start"
          gap={3}
        >
          <Text
            fontWeight="bold"
            fontSize="sm"
            minW="24px"
            color="blue.500"
          >
            {index + 1}.
          </Text>
          <Box>
            <Text fontSize="sm">{step.description}</Text>
            {step.documentation_link && (
              <Link
                href={step.documentation_link}
                isExternal
                fontSize="xs"
                color="blue.500"
                display="inline-flex"
                alignItems="center"
                gap={1}
                mt={1}
              >
                Docs <FiExternalLink />
              </Link>
            )}
          </Box>
        </HStack>
      ))}
    </Box>
  );
};
