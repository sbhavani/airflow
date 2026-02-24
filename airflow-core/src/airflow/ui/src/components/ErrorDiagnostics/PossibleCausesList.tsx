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

import { Badge, Box, HStack, Text } from "@chakra-ui/react";

type PossibleCause = {
  id: string;
  description: string;
  likelihood: "high" | "medium" | "low";
};

type PossibleCausesListProps = {
  causes: PossibleCause[];
};

const likelihoodConfig = {
  high: { colorScheme: "red", label: "HIGH" },
  medium: { colorScheme: "yellow", label: "MEDIUM" },
  low: { colorScheme: "gray", label: "LOW" },
};

export const PossibleCausesList = ({ causes }: PossibleCausesListProps) => {
  // Sort by likelihood: high -> medium -> low
  const sortedCauses = [...causes].sort((a, b) => {
    const order = { high: 0, medium: 1, low: 2 };
    return order[a.likelihood] - order[b.likelihood];
  });

  return (
    <Box as="ul" listStyleType="none" pl={0}>
      {sortedCauses.map((cause) => {
        const likelihood = likelihoodConfig[cause.likelihood];
        return (
          <HStack
            as="li"
            key={cause.id}
            mb={2}
            alignItems="flex-start"
            gap={2}
          >
            <Badge
              colorScheme={likelihood.colorScheme}
              fontSize="xs"
              minW="60px"
              textAlign="center"
            >
              {likelihood.label}
            </Badge>
            <Text fontSize="sm">{cause.description}</Text>
          </HStack>
        );
      })}
    </Box>
  );
};
