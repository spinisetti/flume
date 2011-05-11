package com.cloudera.flume.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import com.cloudera.flume.master.ZKClient;
import com.cloudera.flume.master.ZKInProcessServer;

public class TestZKFlumeNode {
  /**
   * Test a number of ZKClient operations
   */
  @Test
  public void testZKClient() throws Exception {
    File temp = File.createTempFile("flume-zk-test", "");
    temp.delete();
    temp.mkdir();
    temp.deleteOnExit();
    ZKInProcessServer zk = new ZKInProcessServer(3181, temp.getAbsolutePath());
    zk.start();
    ZKClient client = new ZKClient("localhost:3181");
    client.init();
    client.ensureDeleted("/test-flume", -1);
    assertTrue("Delete not successful!",
        client.exists("/test-flume", false) == null);
    client.ensureExists("/test-flume", "hello world".getBytes());
    Stat stat = new Stat();
    byte[] data = client.getData("/test-flume", false, stat);
    String dataString = new String(data);
    assertEquals("Didn't get expected 'hello world' from getData - "
        + dataString, dataString, "hello world");

    client.delete("/test-flume", stat.getVersion());
    assertTrue("Final delete not successful!", client.exists("/test-flume",
        false) == null);
    zk.stop();
  }
}
