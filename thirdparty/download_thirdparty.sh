#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

if [ "x${IMPALA_HOME}" = "x" ]; then
  echo "IMPALA_HOME must be set"
  exit 1;
fi

SIGNATURES=${IMPALA_HOME}/thirdparty/signatures.txt

fetch() {
    echo "Fetching $1"
    filename=${2##*/}
    wget "$2"
    sha1sum=$(sha1sum "$filename")
    if ! grep -qF "$sha1sum" "$SIGNATURES"; then
        expected=$(grep "$filename" "$SIGNATURES")
        echo "WARNING: Invalid sha1sum: $sha1sum. Expected: $expected"
        echo "ABORTING"
        exit 1
    fi
}

cd ${IMPALA_HOME}/thirdparty

echo "Removing everything in ${IMPALA_HOME}/thirdparty"
git clean -xdf

fetch gtest http://googletest.googlecode.com/files/gtest-${IMPALA_GTEST_VERSION}.zip
unzip gtest-${IMPALA_GTEST_VERSION}.zip
rm gtest-${IMPALA_GTEST_VERSION}.zip

fetch glog http://google-glog.googlecode.com/files/glog-${IMPALA_GLOG_VERSION}.tar.gz
tar xzf glog-${IMPALA_GLOG_VERSION}.tar.gz
rm glog-${IMPALA_GLOG_VERSION}.tar.gz

fetch gflags http://gflags.googlecode.com/files/gflags-${IMPALA_GFLAGS_VERSION}.zip
unzip gflags-${IMPALA_GFLAGS_VERSION}.zip
rm gflags-${IMPALA_GFLAGS_VERSION}.zip

fetch gperftools http://gperftools.googlecode.com/files/gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz
tar xzf gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz
rm gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz

fetch snappy http://snappy.googlecode.com/files/snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
tar xzf snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
rm snappy-${IMPALA_SNAPPY_VERSION}.tar.gz

fetch cyrus-sasl ftp://ftp.andrew.cmu.edu/pub/cyrus-mail/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz
tar xzf cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz
rm cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz

fetch mongoose http://mongoose.googlecode.com/files/mongoose-${IMPALA_MONGOOSE_VERSION}.tgz
tar xzf mongoose-${IMPALA_MONGOOSE_VERSION}.tgz
rm mongoose-${IMPALA_MONGOOSE_VERSION}.tgz

fetch "Apache Hadoop" http://archive.cloudera.com/cdh4/cdh/4/hadoop-${IMPALA_HADOOP_VERSION}.tar.gz
tar xzf hadoop-${IMPALA_HADOOP_VERSION}.tar.gz
rm hadoop-${IMPALA_HADOOP_VERSION}.tar.gz

fetch "Apache Hive" http://archive.cloudera.com/cdh4/cdh/4/hive-${IMPALA_HIVE_VERSION}.tar.gz
tar xzf hive-${IMPALA_HIVE_VERSION}.tar.gz
rm hive-${IMPALA_HIVE_VERSION}.tar.gz

fetch "Apache Thrift" http://archive.apache.org/dist/thrift/${IMPALA_THRIFT_VERSION}/thrift-${IMPALA_THRIFT_VERSION}.tar.gz
tar xzf thrift-${IMPALA_THRIFT_VERSION}.tar.gz
rm thrift-${IMPALA_THRIFT_VERSION}.tar.gz
