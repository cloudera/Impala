/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hive.service.Service;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.EmbeddedThriftCLIService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * CLIServiceTest.
 *
 */
public abstract class CLIServiceTest {

  // Wrapper class to expose the CLIService
  public static class EmbeddedCLIServiceForTest extends EmbeddedThriftCLIService {
    public CLIService getCliService() {
      return cliService;
    }
  }

  protected static CLIServiceClient client;
  protected static EmbeddedCLIServiceForTest service;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void createSessionTest() throws Exception {
    SessionHandle sessionHandle = client
        .openSession("tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getFunctionsTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);
    OperationHandle opHandle = client.getFunctions(sessionHandle, null, null, "*");
    TableSchema schema = client.getResultSetMetadata(opHandle);

    ColumnDescriptor columnDesc = schema.getColumnDescriptorAt(0);
    assertEquals("FUNCTION_CAT", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(1);
    assertEquals("FUNCTION_SCHEM", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(2);
    assertEquals("FUNCTION_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(3);
    assertEquals("REMARKS", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(4);
    assertEquals("FUNCTION_TYPE", columnDesc.getName());
    assertEquals(Type.INT_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(5);
    assertEquals("SPECIFIC_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getInfoTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);

    GetInfoValue value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER);
    System.out.println(value.getStringValue());

    client.closeSession(sessionHandle);
  }

  /**
   * Test that execute doesn't fail when confOverlay is null
   * @throws Exception
   */
  @Test
  public void testNullConfOverlay() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);
    String statement = "show databases";
    OperationHandle opHandle = client.executeStatement(sessionHandle, statement, null);
    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  /**
   * Test per statement configuration overlay
   * @throws Exception
   */
  @Test
  public void testConfOverlay() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);
    String statement = "show databases";
    Map <String, String> confOverlay = new HashMap<String, String>();

    // set a pass a property to operation and check if its set the query config
    confOverlay.put("test.foo", "bar");
    OperationHandle opHandle = client.executeStatement(sessionHandle, statement, confOverlay);
    assertNotNull(opHandle);
    Operation sqlOperation = getOperationFromHandle(opHandle);
    assertNotNull(sqlOperation.getConfiguration());
    assertEquals("bar", sqlOperation.getConfiguration().get("test.foo"));
    client.closeOperation(opHandle);

    client.closeSession(sessionHandle);
  }

  // extract the session manager from CLI Service
  protected SessionManager getSessionManager()
      throws HiveSQLException {
    SessionManager sessionManager = null;
    for (Service hiveService : service.getCliService().getServices()) {
      if (hiveService instanceof SessionManager ) {
        sessionManager = (SessionManager)hiveService;
      }
    }
    assertNotNull(sessionManager);
    return sessionManager;
  }

  protected HiveSession getSessionFromHandle(SessionHandle sessionHandle)
      throws HiveSQLException {
    return getSessionManager().getSession(sessionHandle);
  }

  protected Operation getOperationFromHandle(OperationHandle opHandle)
      throws HiveSQLException {
    return getSessionManager().getOperationManager().getOperation(opHandle);
  }

}
