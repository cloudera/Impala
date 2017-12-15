# CLOUDERA-BUILD
export JAVA7_BUILD=true
. /opt/toolchain/toolchain.sh

# TODO: run binary compatibility check

# shellcheck disable=SC2034
MAVEN_HOME=${MAVEN_3_2_2_HOME}

if [[ "true" = "${DEBUG}" ]]; then
  set -x
  env
fi

echo "checking user"
whoami
echo "checking groups"
groups

COMPONENT=${WORKSPACE}/repos/hbase
TEST_FRAMEWORK=${WORKSPACE}/test_framework

# defensive check against misbehaving tests
find "${COMPONENT}" -name target -exec chmod -R u+w {} \;

PATCHPROCESS=${WORKSPACE}/patchprocess
if [[ -d ${PATCHPROCESS} ]]; then
  echo "[WARN] patch process already existed '${PATCHPROCESS}'"
  rm -rf "${PATCHPROCESS}"
fi
mkdir -p "${PATCHPROCESS}"

# First time we call this it's from jenkins, so break it on spaces
YETUS_ARGS=(${YETUS_ARGS} --jenkins)

### Download Yetus
if [ ! -d "${TEST_FRAMEWORK}" ]; then
  echo "[INFO] Downloading Yetus..."
  mkdir -p "${TEST_FRAMEWORK}"
  cd "${TEST_FRAMEWORK}"

  mkdir -p "${TEST_FRAMEWORK}/.gpg"
  chmod -R 700 "${TEST_FRAMEWORK}/.gpg"

  curl -L --fail -o "${TEST_FRAMEWORK}/KEYS_YETUS" https://dist.apache.org/repos/dist/release/yetus/KEYS
  gpg --homedir "${TEST_FRAMEWORK}/.gpg" --import "${TEST_FRAMEWORK}/KEYS_YETUS"

  ## Release
  curl -L --fail -O "https://dist.apache.org/repos/dist/release/yetus/${YETUS_VERSION_NUMBER}/yetus-${YETUS_VERSION_NUMBER}-bin.tar.gz"
  curl -L --fail -O "https://dist.apache.org/repos/dist/release/yetus/${YETUS_VERSION_NUMBER}/yetus-${YETUS_VERSION_NUMBER}-bin.tar.gz.asc"
  gpg --homedir "${TEST_FRAMEWORK}/.gpg" --verify "yetus-${YETUS_VERSION_NUMBER}-bin.tar.gz.asc"
  tar xzpf "yetus-${YETUS_VERSION_NUMBER}-bin.tar.gz"
fi

TESTPATCHBIN=${TEST_FRAMEWORK}/yetus-${YETUS_VERSION_NUMBER}/bin/test-patch
TESTPATCHLIB=${TEST_FRAMEWORK}/yetus-${YETUS_VERSION_NUMBER}/lib/precommit

if [ ! -x "${TESTPATCHBIN}" ] && [ -n "${TEST_FRAMEWORK}" ] && [ -d "${TEST_FRAMEWORK}" ]; then
  echo "Something is amiss with the test framework; removing it. please re-run."
  rm -rf "${TEST_FRAMEWORK}"
  exit 1
fi

# Work around KITCHEN-11523
GIT="${WORKSPACE}/git/bin/git"

cd "${WORKSPACE}"

if [[ "true" = "${DEBUG}" ]]; then
  echo "[DEBUG] debug mode is on, dumping the test patch env"
  # DEBUG print the test framework
  ls -l "${TESTPATCHBIN}"
  ls -la "${TESTPATCHLIB}/test-patch.d/"
  # DEBUG print the local customization
  if [ -d "${COMPONENT}/tools/jenkins/test-patch.d" ]; then
    ls -la "${COMPONENT}/tools/jenkins/test-patch.d/"
  fi
  YETUS_ARGS=(--debug ${YETUS_ARGS[@]})
fi

# Right now running on Docker is broken because it can't find our custom build of git
if [[ "true" = "${RUN_IN_DOCKER}" ]]; then
  YETUS_ARGS=(--docker --findbugs-home=/opt/findbugs ${YETUS_ARGS[@]})
  if [ -d "${COMPONENT}/dev-support/docker/Dockerfile" ]; then
    YETUS_ARGS=(--dockerfile="${COMPONENT}/dev-support/docker/Dockerfile" ${YETUS_ARGS[@]})
  fi
else
  YETUS_ARGS=(--findbugs-home=/opt/toolchain/findbugs-1.3.9 ${YETUS_ARGS[@]})
fi

# If we start including any custom plugins, grab them.
if [ -d "${COMPONENT}/dev-support/test-patch.d" ]; then
  YETUS_ARGS=("--user-plugins=${COMPONENT}/dev-support/test-patch.d" ${YETUS_ARGS[@]})
fi

# If we have our personality defined, use it.
if [ -r "${COMPONENT}/cloudera/cdh-personality.sh" ]; then
  YETUS_ARGS=("--personality=${COMPONENT}/cloudera/cdh-personality.sh" ${YETUS_ARGS[@]})
fi

# work around YETUS-61, manually create a patch file and move the repo to the correct branch.
if [ -z "${GIT_COMMIT}" ] || [ -z "${GERRIT_BRANCH}" ]; then
  echo "[FATAL] env variables about the git commits under test not found, aborting." >&2
  exit 1
fi
PATCHFILE=$(mktemp --quiet --tmpdir="${PATCHPROCESS}" "hbase.precommit.test.XXXXXX-${GERRIT_BRANCH}.patch")
cd "${COMPONENT}"
"${GIT}" format-patch --stdout -1 "${GIT_COMMIT}" >"${PATCHFILE}"
"${GIT}" checkout "${GERRIT_BRANCH}"
# NOTE will break if this is a merge commit
"${GIT}" reset --hard "${GIT_COMMIT}^"
"${GIT}" branch --set-upstream-to="origin/${GERRIT_BRANCH}" "${GERRIT_BRANCH}"
cd "${WORKSPACE}"

# invoke test-patch and send results to a known HTML file.
if ! /bin/bash "${TESTPATCHBIN}" \
        "${YETUS_ARGS[@]}" \
        --patch-dir="${PATCHPROCESS}" \
        --basedir="${COMPONENT}" \
        --mvn-custom-repos \
        --git-cmd="${GIT}" \
        --branch="${GERRIT_BRANCH}" \
        --html-report-file="${PATCHPROCESS}/report_output.html" \
        "${PATCHFILE}" ; then
  echo "[ERROR] test patch failed, grabbing test logs into artifact 'test_logs.zip'"
  echo "[DEBUG] If we failed but didn't run any junit tests, zip will fail. We can safely ignore that."
  find "${COMPONENT}" -path '*/target/surefire-reports/*' | zip "${PATCHPROCESS}/test_logs.zip" -@ || true
  exit 1
fi
