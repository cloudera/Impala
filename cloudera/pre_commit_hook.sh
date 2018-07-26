#!/usr/bin/env bash
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

# This is called by
# https://master-02.jenkins.cloudera.com/job/impala-pre-commit-unittest/
# which is derived by
# http://github.mtv.cloudera.com/Kitchen/jenkins-master/blob/master/dsl/master-02/pre_commit/unittest_gerrit.groovy#L167
# It is triggered on pushes to the gerrit.sjc.cloudera.com "cdh/impala" project.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

# JAVA_HOME and a path to mvn are expected to be set up by the Jenkins job.
if [[ -z "${JAVA_HOME}" ]]
then
  echo "JAVA_HOME is not set"
  exit 1
fi

export PYPI_MIRROR="https://pypi.infra.cloudera.com/api/pypi/pypi-public"
export IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pushd "${IMPALA_HOME}"
source bin/impala-config.sh
# If you alter the buildall.sh line, please ensure the calling Jenkins job handlers proper
# gathering of artifacts. See also
# http://github.mtv.cloudera.com/QE/Impala-auxiliary-tests/blob/master/jenkins/build.sh
# for other inspirations around buildall.sh that may become necessary.
./buildall.sh -notests
