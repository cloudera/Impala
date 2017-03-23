#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Attempt to set a value in the catalina.properties file. Insert new
# attributes; overwrite existing attributes.
# $1 attribute
# The rest - value
# return: 1 on failure, 0 on success
set_catalina_prop() {
  ATTR=$1
  shift
  VALUE="$@"
  if [ ! -e "${CATALINA_BASE}" ]; then
    return 1
  fi
  CATALINA_PROPERTIES="${CATALINA_BASE}/conf/catalina.properties"
  if [ ! -e "${CATALINA_PROPERTIES}" ]; then
    return 1
  fi
  if grep -q "^${ATTR}=" "${CATALINA_PROPERTIES}"; then
    sed -i "s#${ATTR}=.*#${ATTR}=${VALUE}#" "${CATALINA_PROPERTIES}"
    if [ $? -eq 1 ]; then
      # sed somehow fails on the replace; fail so environment is used.
      return 1
    fi
  else
    echo "${ATTR}=${VALUE}" >> "${CATALINA_PROPERTIES}"
  fi
  return 0
}

# Try to set a value in the catalina.properties file. If that fails, set
# it in catalina_opts: an environment variable that becomes a command
# line argument.
# $1 attribute
# The rest - the value
tomcat_set_prop() {
  ATTR=$1
  shift
  VALUE="$@"
  # If no value, don't set anything.
  if [ -z "${VALUE}" ]; then
    return
  fi

  set_catalina_prop "${ATTR}" "${VALUE}"
  if [ $? -eq 1 ]; then
    catalina_opts="${catalina_opts} -D${ATTR}=${VALUE}";
  fi
}

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

source ${HADOOP_LIBEXEC_DIR:-${BASEDIR}/libexec}/httpfs-config.sh

# The Java System property 'httpfs.http.port' it is not used by HttpFS,
# it is used in Tomcat's server.xml configuration file
#
print "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

catalina_opts="-Dproc_httpfs";
catalina_opts="${catalina_opts} -Dhttpfs.home.dir=${HTTPFS_HOME}";
catalina_opts="${catalina_opts} -Dhttpfs.config.dir=${HTTPFS_CONFIG}";
catalina_opts="${catalina_opts} -Dhttpfs.log.dir=${HTTPFS_LOG}";
catalina_opts="${catalina_opts} -Dhttpfs.temp.dir=${HTTPFS_TEMP}";
catalina_opts="${catalina_opts} -Dhttpfs.admin.port=${HTTPFS_ADMIN_PORT}";
catalina_opts="${catalina_opts} -Dhttpfs.http.port=${HTTPFS_HTTP_PORT}";
catalina_opts="${catalina_opts} -Dhttpfs.http.hostname=${HTTPFS_HTTP_HOSTNAME}";

# Try to put SSL items inside catalina.properties; on failure fall back to command line.
tomcat_set_prop httpfs.ssl.enabled "${HTTPFS_SSL_ENABLED}"
tomcat_set_prop httpfs.ssl.keystore.file "${HTTPFS_SSL_KEYSTORE_FILE}"
tomcat_set_prop httpfs.ssl.keystore.pass "${HTTPFS_SSL_KEYSTORE_PASS}"

print "Adding to CATALINA_OPTS:     ${catalina_opts}"

export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export JAVA_OPTS=${CATALINA_OPTS}
fi

if [ "${HTTPFS_SILENT}" != "true" ]; then
  exec ${HTTPFS_CATALINA_HOME}/bin/catalina.sh "$@"
else
  exec ${HTTPFS_CATALINA_HOME}/bin/catalina.sh "$@" > /dev/null
fi

