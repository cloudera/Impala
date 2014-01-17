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

package org.apache.hive.service.cli.thrift;

import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;


/**
 * CLIService.
 *
 */
public class ThriftCLIService extends AbstractService implements TCLIService.Iface, Runnable {

  public static final Log LOG = LogFactory.getLog(ThriftCLIService.class.getName());

  protected CLIService cliService;
  private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);
  private static final TStatus ERROR_STATUS = new TStatus(TStatusCode.ERROR_STATUS);
  private static final TStatus STILL_EXECUTING_STATUS = new TStatus(TStatusCode.STILL_EXECUTING_STATUS);

  private static HiveAuthFactory hiveAuthFactory;

  private int portNum;
  private InetSocketAddress serverAddress;
  private TServer server;

  private boolean isStarted = false;
  protected boolean isEmbedded = false;

  private HiveConf hiveConf;

  private int minWorkerThreads;
  private int maxWorkerThreads;
  private int requestTimeout;



  public ThriftCLIService(CLIService cliService) {
    super("ThriftCLIService");
    this.cliService = cliService;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    if (!isStarted && !isEmbedded) {
      new Thread(this).start();
      isStarted = true;
    }
  }

  @Override
  public synchronized void stop() {
    if (isStarted && !isEmbedded) {
      server.stop();
      isStarted = false;
    }
    super.stop();
  }

  private String getIpAddress() {
    if(hiveAuthFactory != null) {
      return hiveAuthFactory.getIpAddress();
    }
    return SessionManager.getIpAddress();
  }
  
  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
    TOpenSessionResp resp = new TOpenSessionResp();
    try {
      String userName;
      String ipAddress = null;
      if (hiveAuthFactory != null
          && hiveAuthFactory.getRemoteUser() != null) {
        userName = hiveAuthFactory.getRemoteUser();
        ipAddress = hiveAuthFactory.getIpAddress();
      } else {
        userName = SessionManager.getUserName();
      }
      
      if (userName == null) {
        userName = req.getUsername();
      }
      
      userName =  getProxyUser(userName, req.getConfiguration(), getIpAddress());
      
      SessionHandle sessionHandle = null;
      if (cliService.getHiveConf().
          getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_IMPERSONATION)) {
        String delegationTokenStr = null;
        try {
          delegationTokenStr = cliService.getDelegationTokenFromMetaStore(userName);
        } catch (UnsupportedOperationException e) {
          // The delegation token is not applicable in the given deployment mode
        }
        sessionHandle = cliService.openSessionWithImpersonation(userName, req.getPassword(),
              req.getConfiguration(), delegationTokenStr);
      } else {
        sessionHandle = cliService.openSession(userName, req.getPassword(),
              req.getConfiguration());
      }
      // Cannot break the b/w compatibility of API to accept ipAddress as another parameter in
      // openSession call. Hence making this call
      if (ipAddress != null) {
        cliService.setIpAddress(sessionHandle, ipAddress);
      }
      resp.setSessionHandle(sessionHandle.toTSessionHandle());
      // TODO: set real configuration map
      resp.setConfiguration(new HashMap<String, String>());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
    TCloseSessionResp resp = new TCloseSessionResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      cliService.closeSession(sessionHandle);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    TGetInfoResp resp = new TGetInfoResp();
    try {
      GetInfoValue getInfoValue =
          cliService.getInfo(new SessionHandle(req.getSessionHandle()),
              GetInfoType.getGetInfoType(req.getInfoType()));
      resp.setInfoValue(getInfoValue.toTGetInfoValue());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    TExecuteStatementResp resp = new TExecuteStatementResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      String statement = req.getStatement();
      Map<String, String> confOverlay = req.getConfOverlay();
      OperationHandle operationHandle =
          cliService.executeStatement(sessionHandle, statement, confOverlay);
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      if (cliService.getOperationStatus(operationHandle).equals(OperationState.RUNNING)) {
        resp.setStatus(STILL_EXECUTING_STATUS);
      } else {
        resp.setStatus(OK_STATUS);
      }
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
    TGetTypeInfoResp resp = new TGetTypeInfoResp();
    try {
      OperationHandle operationHandle = cliService.getTypeInfo(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
    TGetCatalogsResp resp = new TGetCatalogsResp();
    try {
      OperationHandle opHandle = cliService.getCatalogs(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
    TGetSchemasResp resp = new TGetSchemasResp();
    try {
      OperationHandle opHandle = cliService.getSchemas(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(), req.getSchemaName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
    TGetTablesResp resp = new TGetTablesResp();
    try {
      OperationHandle opHandle = cliService
          .getTables(new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
              req.getSchemaName(), req.getTableName(), req.getTableTypes());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
    TGetTableTypesResp resp = new TGetTableTypesResp();
    try {
      OperationHandle opHandle = cliService.getTableTypes(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
    TGetColumnsResp resp = new TGetColumnsResp();
    try {
      OperationHandle opHandle = cliService.getColumns(
          new SessionHandle(req.getSessionHandle()),
          req.getCatalogName(),
          req.getSchemaName(),
          req.getTableName(),
          req.getColumnName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
    TGetFunctionsResp resp = new TGetFunctionsResp();
    try {
      OperationHandle opHandle = cliService.getFunctions(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
          req.getSchemaName(), req.getFunctionName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    TGetOperationStatusResp resp = new TGetOperationStatusResp();
    try {
      OperationState operationState = cliService.getOperationStatus(new OperationHandle(req.getOperationHandle()));
      resp.setOperationState(operationState.toTOperationState());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    TCancelOperationResp resp = new TCancelOperationResp();
    try {
      cliService.cancelOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    TCloseOperationResp resp = new TCloseOperationResp();
    try {
      cliService.closeOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
    TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
    try {
      TableSchema schema = cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle()));
      resp.setSchema(schema.toTTableSchema());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    TFetchResultsResp resp = new TFetchResultsResp();
    try {
      RowSet rowSet = cliService.fetchResults(
          new OperationHandle(req.getOperationHandle()),
          FetchOrientation.getFetchOrientation(req.getOrientation()),
          req.getMaxRows());
      resp.setResults(rowSet.toTRowSet());
      resp.setHasMoreRows(false);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetLogResp GetLog(TGetLogReq req) throws TException {
    TGetLogResp resp = new TGetLogResp();
    try {
      String log = cliService.getLog(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
      resp.setLog(log);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req)
      throws TException {
    TGetDelegationTokenResp resp = new TGetDelegationTokenResp();

    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        String token = cliService.getDelegationToken(
            new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getOwner(), req.getRenewer());
        if (token == null || token.isEmpty()) {
          throw new HiveSQLException("Got empty token");
        }
        resp.setDelegationToken(token);
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        e.printStackTrace();
        TStatus tokenErrorStatus = HiveSQLException.toTStatus(e);
        tokenErrorStatus.setSqlState("42000");
        resp.setStatus(tokenErrorStatus);
      }
    }
    return resp;
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req)
      throws TException {
    TCancelDelegationTokenResp resp = new TCancelDelegationTokenResp();

    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.cancelDelegationToken(new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getDelegationToken());
      } catch (HiveSQLException e) {
        e.printStackTrace();
        resp.setStatus(HiveSQLException.toTStatus(e));
      }
    }
    return resp;
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req)
      throws TException {
    TRenewDelegationTokenResp resp = new TRenewDelegationTokenResp();
    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.renewDelegationToken(new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getDelegationToken());
      } catch (HiveSQLException e) {
        e.printStackTrace();
        resp.setStatus(HiveSQLException.toTStatus(e));
      }
    }
    return resp;

  }

  private TStatus unsecureTokenErrorStatus() {
    TStatus errorStatus = new TStatus(TStatusCode.ERROR_STATUS);
    errorStatus.setErrorMessage("Delegation token only supported over remote " +
    		"client with kerberos authentication");
    errorStatus.setSqlState("42000");
    return errorStatus;
  }

  @Override
  public void run() {
    try {
      hiveAuthFactory = new HiveAuthFactory();
      TTransportFactory  transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);

      String portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }

      String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
      if (hiveHost == null) {
        hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
      }

      if (hiveHost != null && !hiveHost.isEmpty()) {
        serverAddress = new InetSocketAddress(hiveHost, portNum);
      } else {
        serverAddress = new  InetSocketAddress(portNum);
      }


      minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
      maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
      requestTimeout = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT);


      TServerSocket serverSocket = null;
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname +
              " Not configured for SSL connection");
        }
        serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum,
            keyStorePath, hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD));
      }
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
      .processorFactory(processorFactory)
      .transportFactory(transportFactory)
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads)
      .requestTimeout(requestTimeout);

      server = new TThreadPoolServer(sargs);

      LOG.info("ThriftCLIService listening on " + serverAddress);

      server.serve();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws HiveSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf, String ipAddress)
      throws HiveSQLException {
    if (sessionConf == null || !sessionConf.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      return realUser;
    }

    // Extract the proxy user name and check if we are allowed to do the substitution
    String proxyUser = sessionConf.get(HiveAuthFactory.HS2_PROXY_USER);
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (HiveAuthFactory.AuthTypes.NONE.toString().
        equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    try {
      UserGroupInformation sessionUgi;
      if (!ShimLoader.getHadoopShims().isSecurityEnabled()) {
        sessionUgi = ShimLoader.getHadoopShims().createProxyUser(realUser);
      } else {
        sessionUgi = ShimLoader.getHadoopShims().createRemoteUser(realUser, null);
      }
      ShimLoader.getHadoopShims().
      authorizeProxyAccess(proxyUser, sessionUgi, ipAddress, hiveConf);
      return proxyUser;
    } catch (IOException e) {
      throw new HiveSQLException("Failed to validate proxy privilage of " + realUser +
          " for " + proxyUser, e);
    }
  }
}
