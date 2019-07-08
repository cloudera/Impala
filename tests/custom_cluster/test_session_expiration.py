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
# Tests for query expiration.

import pytest
import socket
from time import sleep

from TCLIService import TCLIService
from getpass import getuser
from tests.hs2.hs2_test_suite import HS2TestSuite
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import DEFAULT_HS2_PORT

class TestSessionExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=6 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    # setup_class creates an Impala client to <hostname>:21000 after the cluster starts.
    # The client expires at the same time as the client created below. Since we choose the
    # impalad to connect to randomly, the test becomes flaky, as the metric we expect to
    # be incremented by 1 gets incremented by 2 if both clients are connected to the same
    # Impalad.
    self.client.close()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
    num_connections = impalad.service.get_metric_value(
        "impala.thrift-server.beeswax-frontend.connections-in-use")
    client = impalad.service.create_beeswax_client()
    # Sleep for half the expiration time to confirm that the session is not expired early
    # (see IMPALA-838)
    sleep(3)
    assert num_expired == impalad.service.get_metric_value(
        "impala-server.num-sessions-expired")
    # Wait for session expiration. Impala will poll the session expiry queue every second
    impalad.service.wait_for_metric_value(
        "impala-server.num-sessions-expired", num_expired + 1, 20)
    # Verify that the idle connection is not closed.
    assert 1 + num_connections == impalad.service.get_metric_value(
        "impala.thrift-server.beeswax-frontend.connections-in-use")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=3 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration_with_set(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.client.close()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test if we can set a shorter timeout than the process-wide option
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=1")
    sleep(2.5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

    # Test if we can set a longer timeout than the process-wide option
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=10")
    sleep(5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")


  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=5 "
       "--idle_client_poll_period_s=0")
  def test_unsetting_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.client.close()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test unsetting IDLE_SESSION_TIMEOUT
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=1")

    # Unset to 5 sec
    client.execute('SET IDLE_SESSION_TIMEOUT=""')
    sleep(2)
    # client session should be alive at this point
    assert num_expired == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")
    sleep(5)
    # now client should have expired
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--idle_session_timeout=10 "
      "--idle_client_poll_period_s=1")
  def test_closing_idle_connection(self, vector):
    """ IMPALA-7802: verifies that connections of idle sessions are closed
    after the sessions have expired."""
    impalad = self.cluster.get_first_impalad()
    self.__close_default_clients()

    for protocol in ['beeswax', 'hiveserver2']:
      num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
      num_connections_metrics_name = \
          "impala.thrift-server.{0}-frontend.connections-in-use".format(protocol)
      num_connections = impalad.service.get_metric_value(num_connections_metrics_name)

      # Connect to Impala using either beeswax or HS2 client and verify the number of
      # opened connections.
      if protocol == 'beeswax':
        client = impalad.service.create_beeswax_client()
        client.execute("select 1")
      else:
        client = impalad.service.create_hs2_client()
        TestSessionExpiration.__open_session_and_run_hs2_query(client, "select 1")

      impalad.service.wait_for_metric_value(num_connections_metrics_name,
           num_connections + 1, 20)

      # Wait till the session has expired.
      impalad.service.wait_for_metric_value("impala-server.num-sessions-expired",
           num_expired + 1, 20)
      # Wait till the idle connection is closed.
      impalad.service.wait_for_metric_value(num_connections_metrics_name,
           num_connections, 5)

    # Verify that connecting to HS2 port without establishing a session will not cause
    # the connection to be closed.
    num_hs2_connections = impalad.service.get_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use")
    sock = socket.socket()
    sock.connect((impalad._get_hostname(), DEFAULT_HS2_PORT))
    impalad.service.wait_for_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use",
        num_hs2_connections + 1, 60)
    # Sleep for some time for the frontend service thread to check for idleness.
    sleep(15)
    assert num_hs2_connections + 1 == impalad.service.get_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use")
    sock.close()

  @staticmethod
  def __open_session_and_run_hs2_query(client, statement):
    """This is method is added to support running HS2 queries using TCLIService.Client. The method was ported from
    asf master branch from hs2_suite"""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = getuser()
    open_session_req.configuration = dict()
    open_session_req.client_protocol = TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6
    resp = client.OpenSession(open_session_req)
    HS2TestSuite.check_response(resp)
    session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = session_handle
    execute_statement_req.statement = statement
    execute_statement_resp = client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

  def __close_default_clients(self):
    """Close the clients that were automatically created by setup_class(). These clients
    can expire during test, which results in metrics that tests depend on changing. Each
    test should create its own clients as needed."""
    self.client.close()
