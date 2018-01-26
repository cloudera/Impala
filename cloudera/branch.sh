#!/bin/bash

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
#
# This script branches Impala in the context of CDH.
# In c6, the branching process allows components
# to execute arbitrary scripts instead of falling back to
# raw string replacement.
#
# The branching process expects all modifications to be added
# to the index, and an exit status of 0 to indicate success.
#
# For more info about other environment variables available
# refer to:
# http://github.mtv.cloudera.com/CDH/cdh/blob/cdh6.x/README_cauldron.md#branching-and-branch-names
set -exu

sed -i "s/$CDH_START_VERSION/$CDH_NEW_VERSION/g" bin/impala-config-branch.sh
sed -i "s/$CDH_START_MAVEN_VERSION/$CDH_NEW_MAVEN_VERSION/g" impala-parent/pom.xml

# Assert something changed and print the diff.
! git diff --exit-code

git add bin/impala-config-branch.sh impala-parent/pom.xml
