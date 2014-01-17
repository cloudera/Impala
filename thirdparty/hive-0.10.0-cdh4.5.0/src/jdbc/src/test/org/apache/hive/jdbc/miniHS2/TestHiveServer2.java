package org.apache.hive.jdbc.miniHS2;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHiveServer2 {

  private static MiniHS2 miniHS2 = null;
  private Map<String, String> confOverlay;

  @BeforeClass
  public static void beforeTest() throws IOException {
    miniHS2 = new MiniHS2(new HiveConf());
  }

  @Before
  public void setUp() throws Exception {
    miniHS2.start();
    confOverlay = new HashMap<String, String>();
  }

  @After
  public void tearDown() {
    miniHS2.stop();
  }

  @Test
  public void testConnection() throws Exception {
    String tabName = "testTab1";
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS tab", confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE TABLE " + tabName + " (id INT)", confOverlay);
    OperationHandle opHandle = serviceClient.executeStatement(sessHandle, "SHOW TABLES", confOverlay);
    RowSet rowSet = serviceClient.fetchResults(opHandle);
    assertFalse(rowSet.getSize() == 0);
  }
}
