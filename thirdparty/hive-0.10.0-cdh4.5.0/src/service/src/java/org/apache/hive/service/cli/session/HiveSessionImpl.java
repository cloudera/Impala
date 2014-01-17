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

package org.apache.hive.service.cli.session;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.log.LogManager;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.GetTypeInfoOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.OperationManager;

/**
 * HiveSession
 *
 */
public class HiveSessionImpl implements HiveSession {

  private final SessionHandle sessionHandle = new SessionHandle();
  private String username;
  private final String password;
  private final Map<String, String> sessionConf = new HashMap<String, String>();
  private final HiveConf hiveConf = new HiveConf();
  private final SessionState sessionState;

  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final Log LOG = LogFactory.getLog(HiveSessionImpl.class);


  private SessionManager sessionManager;
  private OperationManager operationManager;
  private LogManager logManager;
  private IMetaStoreClient metastoreClient = null;
  private String ipAddress;
  private final Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();

  public HiveSessionImpl(String username, String password, Map<String, String> sessionConf, String ipAddress) {
    this.username = username;
    this.password = password;
    this.ipAddress = ipAddress;

    if (sessionConf != null) {
      for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }
    // set an explicit session name to control the download directory name
    hiveConf.set(ConfVars.HIVESESSIONID.varname,
        sessionHandle.getHandleIdentifier().toString());
    sessionState = new SessionState(hiveConf);
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  @Override
  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  protected synchronized void acquire() throws HiveSQLException {
    SessionState.start(sessionState);
  }

  protected synchronized void release() {
    assert sessionState != null;
    // no need to release sessionState...
  }

  @Override
  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public HiveConf getHiveConf() {
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE, FETCH_WORK_SERDE_CLASS);
    return hiveConf;
  }

  @Override
  public LogManager getLogManager() {
    return logManager;
  }

  @Override
  public void setLogManager(LogManager logManager) {
    this.logManager = logManager;
  }

  @Override
  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    if (metastoreClient == null) {
      try {
        metastoreClient = new HiveMetaStoreClient(getHiveConf());
      } catch (MetaException e) {
        throw new HiveSQLException(e);
      }
    }
    return metastoreClient;
  }

  @Override
  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws HiveSQLException {
    acquire();
    try {
      switch (getInfoType) {
      case CLI_SERVER_NAME:
        return new GetInfoValue("Hive");
      case CLI_DBMS_NAME:
        return new GetInfoValue("Apache Hive");
      case CLI_DBMS_VER:
        return new GetInfoValue("0.10.0");
      case CLI_MAX_COLUMN_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_SCHEMA_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_TABLE_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_TXN_CAPABLE:
      default:
        throw new HiveSQLException("Unrecognized GetInfoType value: "  + getInfoType.toString());
      }
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      ExecuteStatementOperation operation = getOperationManager()
          .newExecuteStatementOperation(getSession(), statement, confOverlay);
      //Log capture
      getLogManager().unregisterCurrentThread();
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);

      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetTypeInfoOperation operation = getOperationManager().newGetTypeInfoOperation(getSession());
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getCatalogs() throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetCatalogsOperation operation = getOperationManager().newGetCatalogsOperation(getSession());
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetSchemasOperation operation =
          getOperationManager().newGetSchemasOperation(getSession(), catalogName, schemaName);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes) throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      MetadataOperation operation =
        getOperationManager().newGetTablesOperation(getSession(), catalogName, schemaName, tableName, tableTypes);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
     release();
    }
  }

  @Override
  public OperationHandle getTableTypes() throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetTableTypesOperation operation = getOperationManager().newGetTableTypesOperation(getSession());
      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();
      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
    GetColumnsOperation operation = getOperationManager().newGetColumnsOperation(getSession(),
        catalogName, schemaName, tableName, columnName);
    //Log Capture
    operationHandle = operation.getHandle();
    getLogManager().registerCurrentThread(operationHandle);
    operation.run();

    // unregister the current thread after capturing the log
    getLogManager().unregisterCurrentThread();
    opHandleSet.add(operationHandle);
    return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetFunctionsOperation operation = getOperationManager()
          .newGetFunctionsOperation(getSession(), catalogName, schemaName, functionName);
      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      opHandleSet.add(operationHandle);
      return operationHandle;
    } finally {
      release();
    }
  }

  @Override
  public void close() throws HiveSQLException {
    try {
      acquire();
      /**
       *  For metadata operations like getTables(), getColumns() etc,
       * the session allocates a private metastore handler which should be
       * closed at the end of the session
       */
      if (metastoreClient != null) {
        metastoreClient.close();
      }

      // Iterate through the opHandles and close their operations
      for (OperationHandle opHandle : opHandleSet) {
        operationManager.closeOperation(opHandle);
      }
      opHandleSet.clear();
      HiveHistory hiveHist = sessionState.getHiveHistory();
      if (null != hiveHist) {
        hiveHist.closeStream();
      }
      sessionState.close();
    } catch (IOException ioe) {
      throw new HiveSQLException("Failure to close", ioe);
    } finally {
      release();
    }
  }

  @Override
  public SessionState getSessionState() {
    return sessionState;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public String setIpAddress(String ipAddress) {
    return this.ipAddress = ipAddress;
  }

  @Override
  public String getUserName() {
    return username;
  }

  @Override
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      operationManager.closeOperation(opHandle);
      opHandleSet.remove(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager()
          .getOperationNextRowSet(opHandle, orientation, maxRows);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationNextRowSet(opHandle);
    } finally {
      release();
    }
  }

  protected HiveSession getSession() {
    return this;
  }

  @Override
  public String getDelegationToken(HiveAuthFactory authFactory, String owner, String renewer)
      throws HiveSQLException {
    throw new HiveSQLException("Delegation token access is only allowed with impersonation");
  }

  @Override
  public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    throw new HiveSQLException("Delegation token access is only allowed with impersonation");
  }

  @Override
  public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    throw new HiveSQLException("Delegation token access is only allowed with impersonation");
  }
}
