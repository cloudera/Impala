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

package org.apache.hive.jdbc;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.thrift.EmbeddedThriftCLIService;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * HiveConnection.
 *
 */
public class HiveConnection implements java.sql.Connection {
  private static final String HIVE_AUTH_TYPE= "auth";
  private static final String HIVE_AUTH_QOP = "sasl.qop";
  private static final String HIVE_AUTH_SIMPLE = "noSasl";
  private static final String HIVE_AUTH_TOKEN = "delegationToken";
  private static final String HIVE_AUTH_USER = "user";
  private static final String HIVE_AUTH_PRINCIPAL = "principal";
  private static final String HIVE_AUTH_PASSWD = "password";
  private static final String HIVE_ANONYMOUS_USER = "anonymous";
  private static final String HIVE_ANONYMOUS_PASSWD = "anonymous";
  private static final String HIVE_USE_SSL = "ssl";
  private static final String HIVE_SSL_TRUST_STORE = "sslTrustStore";
  private static final String HIVE_SSL_TRUST_STORE_PASSWORD = "trustStorePassword";

  private TTransport transport;
  private TCLIService.Iface client;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TSessionHandle sessHandle = null;
  private final List<TProtocolVersion> supportedProtocols = new LinkedList<TProtocolVersion>();
  private int loginTimeout = 0;
  /**
   * TODO: - parse uri (use java.net.URI?).
   */
  public HiveConnection(String uri, Properties info) throws SQLException {
    Utils.JdbcConnectionParams connParams;
    setupLoginTimeout();
    try {
      connParams = Utils.parseURL(uri);
    } catch (IllegalArgumentException e) {
      throw new SQLException(e);
    }
    if (connParams.isEmbeddedMode()) {
      client = new EmbeddedThriftCLIService();
    } else {
      // extract user/password from JDBC connection properties if its not supplied in the connection URL
      if (info.containsKey(HIVE_AUTH_USER)) {
        connParams.getSessionVars().put(HIVE_AUTH_USER, info.getProperty(HIVE_AUTH_USER));
        if (info.containsKey(HIVE_AUTH_PASSWD)) {
            connParams.getSessionVars().put(HIVE_AUTH_PASSWD, info.getProperty(HIVE_AUTH_PASSWD));
        }
      }
      if (info.containsKey(HIVE_AUTH_TYPE)) {
        connParams.getSessionVars().put(HIVE_AUTH_TYPE, info.getProperty(HIVE_AUTH_TYPE));
      }

      openTransport(uri, connParams.getHost(), connParams.getPort(), connParams.getSessionVars());
    }

    // currently only V1 is supported
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);

    // open client session
    openSession(uri, connParams.getSessionVars());

    configureConnection(connParams);
  }

  private void configureConnection(Utils.JdbcConnectionParams connParams)
      throws SQLException {
    // set the hive variable in session state for local mode
    if (connParams.isEmbeddedMode()) {
      if (!connParams.getHiveVars().isEmpty()) {
        SessionState.get().setHiveVariables(connParams.getHiveVars());
      }
    } else {
      // for remote JDBC client, try to set the conf var using 'set foo=bar'
      Statement stmt = createStatement();
      for (Entry<String, String> hiveConf : connParams.getHiveConfs().entrySet()) {
        stmt.execute("set " + hiveConf.getKey() + "=" + hiveConf.getValue());
        stmt.close();
      }
    }
  }

  private void openTransport(String uri, String host, int port, Map<String, String> sessConf )
      throws SQLException {

    // handle secure connection if specified
    try {
      if (!sessConf.containsKey(HIVE_AUTH_TYPE)
        || !sessConf.get(HIVE_AUTH_TYPE).equals(HIVE_AUTH_SIMPLE)) {
        String tokenStr;
        if (sessConf.containsKey(HIVE_AUTH_PRINCIPAL)) {
          Map<String, String> saslProps = new HashMap<String, String>();
          SaslQOP saslQOP = SaslQOP.AUTH;
          if(sessConf.containsKey(HIVE_AUTH_QOP)) {
            try {
              saslQOP = SaslQOP.fromString(sessConf.get(HIVE_AUTH_QOP));
            } catch (IllegalArgumentException e) {
              throw new SQLException("Invalid " + HIVE_AUTH_QOP + " parameter. " + e.getMessage(), "42000", e);
            }
          }
          saslProps.put(Sasl.QOP, saslQOP.toString());
          saslProps.put(Sasl.SERVER_AUTH, "true");
          transport = KerberosSaslHelper.getKerberosTransport(
                  sessConf.get(HIVE_AUTH_PRINCIPAL), host,
                  HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps);
        } else if ((tokenStr = getClientDelegationToken(sessConf)) != null) {
          transport = KerberosSaslHelper.getTokenTransport(tokenStr,
                  host, HiveAuthFactory.getSocketTransport(host, port, loginTimeout));
        } else {
          String userName = sessConf.get(HIVE_AUTH_USER);
          if ((userName == null) || userName.isEmpty()) {
            userName = HIVE_ANONYMOUS_USER;
          }
          String passwd = sessConf.get(HIVE_AUTH_PASSWD);
          if ((passwd == null) || passwd.isEmpty()) {
            passwd = HIVE_ANONYMOUS_PASSWD;
          }
          String useSslStr = sessConf.get(HIVE_USE_SSL);
          if ("true".equalsIgnoreCase(useSslStr)) {
            String sslTrustStore = sessConf.get(HIVE_SSL_TRUST_STORE);
            String sslTrustStorePassword = sessConf.get(HIVE_SSL_TRUST_STORE_PASSWORD);
            if (sslTrustStore == null || sslTrustStore.isEmpty()) {
              transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout);
            } else {
              transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout,
                  sslTrustStore, sslTrustStorePassword);
            }
          } else {
            transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout);
          }
          transport = PlainSaslHelper.getPlainTransport(userName, passwd, transport);
        }
      } else {
        transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout);
      }
    } catch (SaslException e) {
      throw new SQLException("Could not establish secure connection to "
                + uri + ": " + e.getMessage(), " 08S01", e);
    } catch (TTransportException e) {
      throw new SQLException("Could not create connection to "
          + uri + ": " + e.getMessage(), " 08S01", e);
    }

    TProtocol protocol = new TBinaryProtocol(transport);
    client = new TCLIService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new SQLException("Could not establish connection to "
          + uri + ": " + e.getMessage(), " 08S01", e);
    }
  }

  // Lookup the delegation token. First in the connection URL, then Configuration
  private String getClientDelegationToken(Map<String, String> jdbcConnConf)
      throws SQLException {
    String tokenStr = null;
    if (HIVE_AUTH_TOKEN.equalsIgnoreCase(jdbcConnConf.get(HIVE_AUTH_TYPE))) {
      // check delegation token in job conf if any
      try {
        tokenStr = ShimLoader.getHadoopShims().getTokenStrForm(HiveAuthFactory.HS2_CLIENT_TOKEN);
      } catch (IOException e) {
        throw new SQLException("Error reading token ", e);
      }
    }
    return tokenStr;
  }

  private void openSession(String uri, Map<String, String> sessVars) throws SQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();

    // set the session configuration
    // openReq.setConfiguration(null);
    if (sessVars.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      Map <String, String> openConf = new HashMap<String, String>();
      openConf.put(HiveAuthFactory.HS2_PROXY_USER,
          sessVars.get(HiveAuthFactory.HS2_PROXY_USER));
      openReq.setConfiguration(openConf);
    }

    try {
      TOpenSessionResp openResp = client.OpenSession(openReq);
      // validate connection
      Utils.verifySuccess(openResp.getStatus());
      if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
        throw new TException("Unsupported Hive2 protocol");
      }
    sessHandle = openResp.getSessionHandle();
    } catch (TException e) {
      throw new SQLException("Could not establish connection to "
          + uri + ": " + e.getMessage(), " 08S01", e);
    }
    isClosed = false;
  }

  // copy loginTimeout from driver manager. Thrift timeout needs to be in millis
  private void setupLoginTimeout() {
    long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());
    if (timeOut > Integer.MAX_VALUE) {
      loginTimeout = Integer.MAX_VALUE;
    } else {
      loginTimeout = (int) timeOut;
    }
  }

  public void abort(Executor executor) throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

    public String getDelegationToken(String owner, String renewer) throws SQLException {
      TGetDelegationTokenReq req = new TGetDelegationTokenReq(sessHandle, owner, renewer);
      try {
        TGetDelegationTokenResp tokenResp = client.GetDelegationToken(req);
        Utils.verifySuccess(tokenResp.getStatus());
        return tokenResp.getDelegationToken();
      } catch (TException e) {
        throw new SQLException("Could not retrieve token: " +
            e.getMessage(), " 08S01", e);
      }
    }

    public void cancelDelegationToken(String tokenStr) throws SQLException {
      TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(sessHandle, tokenStr);
      try {
        TCancelDelegationTokenResp cancelResp =
              client.CancelDelegationToken(cancelReq);
        Utils.verifySuccess(cancelResp.getStatus());
        return;
      } catch (TException e) {
        throw new SQLException("Could not cancel token: " +
            e.getMessage(), " 08S01", e);
      }
    }

    public void renewDelegationToken(String tokenStr) throws SQLException {
      TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(sessHandle, tokenStr);
      try {
        TRenewDelegationTokenResp renewResp =
              client.RenewDelegationToken(cancelReq);
        Utils.verifySuccess(renewResp.getStatus());
        return;
      } catch (TException e) {
        throw new SQLException("Could not renew token: " +
            e.getMessage(), " 08S01", e);
      }
    }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#clearWarnings()
   */

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#close()
   */

  public void close() throws SQLException {
    if (!isClosed) {
      TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
      try {
        client.CloseSession(closeReq);
      } catch (TException e) {
        throw new SQLException("Error while cleaning up the server resources", e);
      } finally {
        isClosed = true;
        if (transport != null) {
          transport.close();
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#commit()
   */

  public void commit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createArrayOf(java.lang.String,
   * java.lang.Object[])
   */

  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createBlob()
   */

  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createClob()
   */

  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createNClob()
   */

  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createSQLXML()
   */

  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * Creates a Statement object for sending SQL statements to the database.
   *
   * @throws SQLException
   *           if a database access error occurs.
   * @see java.sql.Connection#createStatement()
   */

  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new HiveStatement(client, sessHandle);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int)
   */

  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException("Method not supported");
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException("Method not supported");
    }
    return new HiveStatement(client, sessHandle,
        resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int, int)
   */

  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
   */

  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getAutoCommit()
   */

  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getCatalog()
   */

  public String getCatalog() throws SQLException {
    return "";
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo()
   */

  public Properties getClientInfo() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo(java.lang.String)
   */

  public String getClientInfo(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getHoldability()
   */

  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getMetaData()
   */

  public DatabaseMetaData getMetaData() throws SQLException {
    return new HiveDatabaseMetaData(client, sessHandle);
  }

  public int getNetworkTimeout() throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }


  public String getSchema() throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTransactionIsolation()
   */

  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTypeMap()
   */

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getWarnings()
   */

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isClosed()
   */

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isReadOnly()
   */

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isValid(int)
   */

  public boolean isValid(int timeout) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#nativeSQL(java.lang.String)
   */

  public String nativeSQL(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String)
   */

  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
   */

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String)
   */

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new HivePreparedStatement(client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int)
   */

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new HivePreparedStatement(client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
   */

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String,
   * java.lang.String[])
   */

  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
   */

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    return new HivePreparedStatement(client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
   */

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback()
   */

  public void rollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */

  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setAutoCommit(boolean)
   */

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLException("enabling autocommit is not supported");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setCatalog(java.lang.String)
   */

  public void setCatalog(String catalog) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.util.Properties)
   */

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setHoldability(int)
   */

  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setReadOnly(boolean)
   */

  public void setReadOnly(boolean readOnly) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint()
   */

  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */

  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  public void setSchema(String schema) throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTransactionIsolation(int)
   */

  public void setTransactionIsolation(int level) throws SQLException {
    // TODO: throw an exception?
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTypeMap(java.util.Map)
   */

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

}
