package org.apache.hive.jdbc.miniHS2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/***
 * Base class for Hive service
 * AbstarctHiveService.
 *
 */
public abstract class AbstarctHiveService {
  private HiveConf hiveConf = null;
  private String hostname;
  private int port;
  private boolean startedHiveService = false;

  public AbstarctHiveService(HiveConf hiveConf, String hostname, int port) {
    this.hiveConf = hiveConf;
    this.hostname = hostname;
    this.port = port;
  }

  /**
   * Get Hive conf
   * @return
   */
  public HiveConf getHiveConf() {
    return hiveConf;
  }

  /**
   * Get config property
   * @param propertyKey
   * @return
   */
  public String getConfProperty(String propertyKey) {
    return hiveConf.get(propertyKey);
  }

  /**
   * Set config property
   * @param propertyKey
   * @param propertyValue
   */
  public void setConfProperty(String propertyKey, String propertyValue) {
    System.setProperty(propertyKey, propertyValue);
    hiveConf.set(propertyKey, propertyValue);
  }

  /**
   * Retrieve warehouse directory
   * @return
   */
  public Path getWareHouseDir() {
    return new Path(hiveConf.getVar(ConfVars.METASTOREWAREHOUSE));
  }

  public void setWareHouseDir(String wareHouseURI) {
    verifyNotStarted();
    System.setProperty(ConfVars.METASTOREWAREHOUSE.varname, wareHouseURI);
    hiveConf.setVar(ConfVars.METASTOREWAREHOUSE, wareHouseURI);
  }

  /**
   * Set service host
   * @param hostName
   */
  public void setHost(String hostName) {
    this.hostname = hostName;
  }

  // get service host
  protected String getHost() {
    return hostname;
  }

  /**
   * Set service port #
   * @param portNum
   */
  public void setPort(int portNum) {
    this.port = portNum;
  }

  // get service port#
  protected int getPort() {
    return port;
  }

  public boolean isStarted() {
    return startedHiveService;
  }

  protected void setStarted(boolean hiveServiceStatus) {
    this.startedHiveService =  hiveServiceStatus;
  }

  protected void verifyStarted() {
    if (!isStarted()) {
      throw new IllegalStateException("HS2 is not running");
    }
  }

  protected void verifyNotStarted() {
    if (isStarted()) {
      throw new IllegalStateException("HS2 alreadyrunning");
    }
  }

}
