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

cmake_minimum_required(VERSION 2.6)

INCLUDE_DIRECTORIES($ENV{IMPALA_HOME}/thirdparty/)
INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIR})

add_library(gutil
bits.cc
stringprintf.cc
strings/memutil.cc
strings/charset.cc
strings/util.cc
strings/join.cc
strings/numbers.cc
strings/split.cc
strings/ascii_ctype.cc
strings/escaping.cc
strings/stringpiece.cc
strings/substitute.cc
strings/strcat.cc
mathlimits.cc
int128.cc
strtoint.cc
hash/hash.cc
hash/jenkins.cc
hash/city.cc
utf/rune.c
)

target_link_libraries(gutil
  rt) # clock_gettime() requires -lrt

# Disable warnings which trigger a lot in the Google code:
set_target_properties(gutil PROPERTIES
  COMPILE_FLAGS "-funsigned-char -Wno-deprecated -Wno-char-subscripts -fPIC")

set(GUTIL_LIBS gutil)
