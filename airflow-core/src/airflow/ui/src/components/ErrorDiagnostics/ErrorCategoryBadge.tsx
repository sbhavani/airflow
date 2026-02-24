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

import { Badge, HStack, Icon, Text } from "@chakra-ui/react";
import { FiClock, FiDatabase, FiLock, FiSettings, FiShield, FiSliders, FiZap } from "react-icons/fi";

type ErrorCategory = "CONNECTION" | "AUTHENTICATION" | "CONFIGURATION" | "RESOURCE" | "UPSTREAM" | "DATA" | "TIMEOUT" | "UNKNOWN";

type ErrorCategoryBadgeProps = {
  category: string;
  size?: "sm" | "md" | "lg";
};

const categoryConfig: Record<ErrorCategory, { color: string; icon: typeof FiZap; label: string }> = {
  CONNECTION: {
    color: "orange",
    icon: FiZap,
    label: "Connection",
  },
  AUTHENTICATION: {
    color: "red",
    icon: FiLock,
    label: "Authentication",
  },
  CONFIGURATION: {
    color: "purple",
    icon: FiSettings,
    label: "Configuration",
  },
  RESOURCE: {
    color: "yellow",
    icon: FiSliders,
    label: "Resource",
  },
  UPSTREAM: {
    color: "blue",
    icon: FiShield,
    label: "Upstream",
  },
  DATA: {
    color: "cyan",
    icon: FiDatabase,
    label: "Data",
  },
  TIMEOUT: {
    color: "yellow",
    icon: FiClock,
    label: "Timeout",
  },
  UNKNOWN: {
    color: "gray",
    icon: FiZap,
    label: "Unknown",
  },
};

export const ErrorCategoryBadge = ({ category, size = "sm" }: ErrorCategoryBadgeProps) => {
  const config = categoryConfig[category as ErrorCategory] || categoryConfig.UNKNOWN;
  const { color, icon, label } = config;

  const sizeMap = {
    sm: { fontSize: "xs", px: 2, py: 0.5 },
    md: { fontSize: "sm", px: 3, py: 1 },
    lg: { fontSize: "md", px: 4, py: 1.5 },
  };

  return (
    <HStack spacing={1}>
      <Badge
        colorScheme={color}
        {...sizeMap[size]}
        borderRadius="full"
        display="flex"
        alignItems="center"
        gap={1}
      >
        <Icon as={icon} aria-hidden="true" />
        <Text as="span">{label}</Text>
      </Badge>
    </HStack>
  );
};
