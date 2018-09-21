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

# This module contains utility functions for testing Parquet files,
# and other functions used for checking for strings in files and
# directories.

import os
from subprocess import check_call

from tests.util.filesystem_utils import get_fs_path


def grep_dir(dir, search):
  '''Recursively search for files that contain 'search' and return a list of matched
     lines grouped by file.
  '''
  matching_files = dict()
  for dir_name, _, file_names in os.walk(dir):
    for file_name in file_names:
      file_path = os.path.join(dir_name, file_name)
      if os.path.islink(file_path):
        continue
      with open(file_path) as file:
        matching_lines = grep_file(file, search)
        if matching_lines:
          matching_files[file_name] = matching_lines
  return matching_files


def grep_file(file, search):
  '''Return lines in 'file' that contain the 'search' term. 'file' must already be
     opened.
  '''
  matching_lines = list()
  for line in file:
    if search in line:
      matching_lines.append(line)
  return matching_lines


def assert_file_in_dir_contains(dir, search):
  '''Asserts that at least one file in the 'dir' contains the 'search' term.'''
  results = grep_dir(dir, search)
  assert results, "%s should have a file containing '%s' but no file was found" \
      % (dir, search)


def assert_no_files_in_dir_contain(dir, search):
  '''Asserts that no files in the 'dir' contains the 'search' term.'''
  results = grep_dir(dir, search)
  assert not results, \
      "%s should not have any file containing '%s' but a file was found" \
      % (dir, search)
