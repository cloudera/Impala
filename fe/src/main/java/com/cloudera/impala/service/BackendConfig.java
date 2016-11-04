// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.service;

import com.cloudera.impala.thrift.TBackendGflags;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;
import org.apache.hadoop.security.authentication.util.KerberosName;

/**
 * This class is meant to provide the FE with impalad backend configuration parameters,
 * including command line arguments.
 * TODO: Remove this class and either
 * a) Figure out if there's a standard way to access flags from java
 * b) Create a util/gflags.java that let's us access the be flags
 */
public class BackendConfig {
  public static BackendConfig INSTANCE;

  private TBackendGflags backendCfg_;

  private BackendConfig(TBackendGflags cfg) {
    backendCfg_ = cfg;
  }

  public static void create(TBackendGflags cfg) {
    Preconditions.checkNotNull(cfg);
    INSTANCE = new BackendConfig(cfg);
    initAuthToLocal();
  }

  public long getReadSize() { return backendCfg_.read_size; }
  public boolean getComputeLineage() {
    return !Strings.isNullOrEmpty(backendCfg_.lineage_event_log_dir);
  }
  public long getIncStatsMaxSize() { return backendCfg_.inc_stats_size_limit_bytes; }
  public boolean isAuthToLocalEnabled() {
    return backendCfg_.load_auth_to_local_rules &&
        !Strings.isNullOrEmpty(backendCfg_.principal);
  }

  // Inits the auth_to_local configuration in the static KerberosName class.
  private static void initAuthToLocal() {
    // If auth_to_local is enabled, we read the configuration hadoop.security.auth_to_local
    // from core-site.xml and use it for principal to short name conversion. If it is not,
    // we use the defaultRule ("RULE:[1:$1] RULE:[2:$1]"), which just extracts the user
    // name from any principal of form a@REALM or a/b@REALM. If auth_to_local is enabled
    // and hadoop.security.auth_to_local is not specified in the hadoop configs, we use
    // the "DEFAULT" rule that just extracts the username from any principal in the
    // cluster's local realm. For more details on principal to short name translation,
    // refer to org.apache.hadoop.security.KerberosName.
    final String defaultRule = "RULE:[1:$1] RULE:[2:$1]";
    final Configuration conf = new Configuration();
    if (INSTANCE.isAuthToLocalEnabled()) {
      KerberosName.setRules(conf.get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
    } else {
      // just extract the simple user name
      KerberosName.setRules(defaultRule);
    }
  }
}
