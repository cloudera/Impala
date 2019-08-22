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

import pytest
import json
import time
import requests

from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, \
    SkipIfLocal
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.util.hive_utils import HiveDbWrapper
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestEventProcessing(CustomClusterTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""
  CATALOG_URL = "http://localhost:25020"
  PROCESSING_TIMEOUT_S = 10

  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1"
  )
  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  def _run_test_empty_partition_events(self, unique_database):
    TBLPROPERTIES = ""
    test_tbl = unique_database + ".test_events"
    self.run_stmt_in_hive("create table {0} (key string, value string) \
      partitioned by (year int) {1} stored as parquet".format(test_tbl, TBLPROPERTIES))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    self.client.execute("describe {0}".format(test_tbl))

    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')

    self.run_stmt_in_hive(
      "alter table {0} add if not exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop if exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"


  def wait_for_insert_event_processing(self, previous_event_id):
    """Waits until the event processor has finished processing insert events. Since two
    events are created for every insert done through hive, we wait until the event id is
    incremented by at least two. Returns true if at least two events were processed within
    self.PROCESSING_TIMEOUT_S, False otherwise.
    """
    new_event_id = self.get_last_synced_event_id()
    success = False
    end_time = time.time() + self.PROCESSING_TIMEOUT_S
    while time.time() < end_time:
      new_event_id = self.get_last_synced_event_id()
      if new_event_id - previous_event_id >= 2:
        success = True
        break
      time.sleep(0.1)
    # Wait for catalog update to be propagated.
    time.sleep(build_flavor_timeout(2, slow_build_timeout=4))
    return success

  def get_event_processor_metrics(self):
    """Scrapes the catalog's /events webpage and return a dictionary with the event
    processor metrics."""
    response = requests.get("%s/events?json" % self.CATALOG_URL)
    assert response.status_code == requests.codes.ok
    varz_json = json.loads(response.text)
    metrics = varz_json["event_processor_metrics"].strip().splitlines()

    # Helper to strip a pair of elements
    def strip_pair(p):
      return (p[0].strip(), p[1].strip())

    pairs = [strip_pair(kv.split(':')) for kv in metrics if kv]
    return dict(pairs)

  def get_last_synced_event_id(self):
    """Returns the last_synced_event_id."""
    metrics = self.get_event_processor_metrics()
    assert 'last-synced-event-id' in metrics.keys()
    return int(metrics['last-synced-event-id'])
