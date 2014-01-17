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

//The tests here are heavily based on some timing, so there is some chance to fail.
package org.apache.hive.service.server;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HiveConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHS2ThreadAllocation {

  private static HiveServer2 hiveServer2;
  private static final int MAX_THREADS = 10;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS, 1);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS, MAX_THREADS);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, 2);

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(2000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  class QueryRunner implements Runnable {

    private int waitTime = 0;

    QueryRunner(int delay) {
      waitTime = delay;
    }

    QueryRunner() {
    }


    @Override
    public void run() {

      HiveConnection connection = null;
      Statement statement = null;
      try {
        connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());

        statement = connection.createStatement();
        if (statement.execute("show tables")) {
          ResultSet results = statement.getResultSet();
          while (results.next()) {
            System.out.println(results.getString(1));
          }
          results.close();
        }
        statement.close();

        if (waitTime > 0) {
          Thread.sleep(waitTime);
        }

        statement = connection.createStatement();
        if (statement.execute("show tables")) {
          ResultSet results = statement.getResultSet();
          while (results.next()) {
            System.out.println(results.getString(1));
          }
          results.close();
        }
        statement.close();
        connection.close();

      } catch (SQLException e) {
        e.printStackTrace();

      } catch (InterruptedException e) {
        e.printStackTrace();

      } finally {
        try {
          if (statement != null) {
            statement.close();
          }

          if (connection != null) {
            connection.close();
          }
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testConnection() throws SQLException {
    int i = MAX_THREADS;
    for (; i > 0; i--) {

      HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      Statement statement = connection.createStatement();

      if (statement.execute("show tables")) {
        ResultSet results = statement.getResultSet();
        while (results.next()) {
          System.out.println(results.getString(1));
        }
        results.close();
      }
      statement.close();
      connection.close();
    }
    Assert.assertEquals(0, i);
  }


  @Test
  public void testParallelConnections() throws InterruptedException {
    List<Thread> threadList = new ArrayList<Thread>(10);
    for (int i = 0; i < MAX_THREADS; i++) {
      Thread thread = new Thread(new QueryRunner());
      thread.setName("HiveConnectionTest-" + i);
      thread.start();
      threadList.add(thread);
      Thread.sleep(100);
    }

    // Wait for all threads to complete/die
    for (Thread thread : threadList) {
      thread.join();
      System.out.println(thread.getName() + " " + thread.isAlive());
    }
  }

  @Test
  public void testHS2StabilityOnLargeConnections() throws InterruptedException {
    List<Thread> threadList = new ArrayList<Thread>(10);
    HiveConnection connection = null;

    for (int i = 0; i < MAX_THREADS; i++) {
      Thread thread = new Thread(new QueryRunner(5000));
      thread.setName("HiveConnectionTest-" + i);
      threadList.add(thread);
      thread.start();
    }

    // Above threads should have exploited the threads in HS2
    Thread.sleep(100);
    // Create another thread and see if the connection goes through.

    boolean connectionFailed = false;
    try {
      connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    } catch (SQLException e) {
      //e.printStackTrace();
      System.out.println("Expected connection failure:" + e.getMessage());
      connectionFailed = true;
    }
    Assert.assertEquals(connectionFailed, true);

    // Hope all threads should be active at this time  -- based on timing, not a good test
    for (Thread thread : threadList) {
      Assert.assertEquals(thread.isAlive(), true);
    }

    // Wait for all threads to complete/die
    for (Thread thread : threadList) {
      System.out.println(thread.getName() + " " + thread.isAlive());
      thread.join();
    }

    // Check if a new connection is possible
    try {
      connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail("Cannot create connection with free threads");
    }
    Assert.assertNotNull(connection);
    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail("Something went wrong with connection closure");
    }
  }
}

