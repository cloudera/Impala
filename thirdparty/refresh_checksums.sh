#!/bin/bash

set -e

if [ "x${IMPALA_HOME}" = "x" ]; then
  echo "IMPALA_HOME must be set"
  exit 1;
fi

SIGNATURES=${IMPALA_HOME}/thirdparty/signatures.txt
TMPDIR=${IMPALA_HOME}/thirdparty/tmp

echo "Initializing temporary directory $TMPDIR"
rm -rf "$TMPDIR"
mkdir "$TMPDIR"
cd "$TMPDIR"

echo "Clearing $SIGNATURES"
rm -f $SIGNATURES

function get_checksum() {
    echo "Checksumming $1"
    filename=${2##*/}
    wget "$2"
    sha1sum "$filename" >> "$SIGNATURES"
}

get_checksum gtest http://googletest.googlecode.com/files/gtest-${IMPALA_GTEST_VERSION}.zip
get_checksum glog http://google-glog.googlecode.com/files/glog-${IMPALA_GLOG_VERSION}.tar.gz
get_checksum gflags http://gflags.googlecode.com/files/gflags-${IMPALA_GFLAGS_VERSION}.zip
get_checksum gperftools http://gperftools.googlecode.com/files/gperftools-${IMPALA_GPERFTOOLS_VERSION}.tar.gz
get_checksum snappy http://snappy.googlecode.com/files/snappy-${IMPALA_SNAPPY_VERSION}.tar.gz
get_checksum cyrus-sasl ftp://ftp.andrew.cmu.edu/pub/cyrus-mail/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}.tar.gz
get_checksum mongoose http://mongoose.googlecode.com/files/mongoose-${IMPALA_MONGOOSE_VERSION}.tgz
get_checksum "Apache Hadoop" http://archive.cloudera.com/cdh4/cdh/4/hadoop-${IMPALA_HADOOP_VERSION}.tar.gz
get_checksum "Apache Hive" http://archive.cloudera.com/cdh4/cdh/4/hive-${IMPALA_HIVE_VERSION}.tar.gz
get_checksum "Apache Thrift" http://archive.apache.org/dist/thrift/${IMPALA_THRIFT_VERSION}/thrift-${IMPALA_THRIFT_VERSION}.tar.gz

echo "Removing $TMPDIR"
rm -rf "$TMPDIR"
