/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

import javax.security.auth.login.AppConfigurationEntry;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SecurityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    String doAs = System.getenv("HADOOP_USER_NAME");
    if (doAs != null && doAs.length() > 0) {
      /*
       * this allows doAs (proxy user) to be passed along across process boundary where
       * delegation tokens are not supported.  For example, a DDL stmt via WebHCat with
       * a doAs parameter, forks to 'hcat' which needs to start a Session that
       * proxies the end user
       */
      return UserGroupInformation.createProxyUser(doAs, UserGroupInformation.getLoginUser());
    }
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * @return the user name set in hadoop.job.ugi param or the current user from System
   * @throws IOException if underlying Hadoop call throws LoginException
   */
  public static String getUser() throws IOException {
    try {
      UserGroupInformation ugi = getUGI();
      return ugi.getUserName();
    } catch (LoginException le) {
      throw new IOException(le);
    }
  }
}
