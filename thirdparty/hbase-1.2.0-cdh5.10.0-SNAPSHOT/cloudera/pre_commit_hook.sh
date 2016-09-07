# CLOUDERA-BUILD
export JAVA7_BUILD=true
. /opt/toolchain/toolchain.sh

# TODO: run binary compatibility check

# Run few client tests (create/delete table, get/put data, ...)
mvn clean test -Dtest=TestAdmin1,TestAdmin2,TestFromClientSide

