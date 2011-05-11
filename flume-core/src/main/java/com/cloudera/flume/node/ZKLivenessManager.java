package com.cloudera.flume.node;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.agent.LogicalNodeManager;
import com.cloudera.flume.agent.MasterRPC;
import com.cloudera.flume.agent.WALAckManager;
import com.cloudera.flume.agent.durability.WALCompletionNotifier;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.google.common.base.Preconditions;

public class ZKLivenessManager implements Watcher {
  String znodeBase = "/flume";
  Logger LOG = LoggerFactory.getLogger(ZKLivenessManager.class);
  protected ZooKeeper zk;
  String zkhosts;
  String host; // local hostname
  String physicalName; // physical node name

  String LOGICALNODES = "logicalNodes";
  String PHYSICALNODES = "physicalNodes";

  int BACKOFF_MILLIS = 5000;

  String logicalZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host;
  }

  String logicalStateZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host + "/state";
  }

  String logicalSinkZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host + "/sink";
  }

  String logicalSourceZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host + "/source";
  }

  String logicalJsonZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host + "/json-url";
  }

  String logicalHttpZNode() {
    return znodeBase + "/" + LOGICALNODES + "/" + host + "/http-url";
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.debug(event.toString());
    String path = event.getPath();
    KeeperState state = event.getState();
    EventType type = event.getType();

    // parse path
    String[] parts = path.split("/");
    if (!znodeBase.equals(parts[0])) {
      // not stuff I care about, bail out.
      return;
    }

    switch (state) {
    case Disconnected:
    case Expired:
    case SyncConnected:
      break;
    default:
      throw new IllegalStateException("bad KeeperState " + state);
    }

    switch (type) {
    case NodeCreated:
    case NodeDeleted:
    case NodeDataChanged:
    case NodeChildrenChanged:
      break;
    default:
      throw new IllegalStateException("bad EvenType " + type);
    }
  }

  /**
   * Establishes a connection with a cluster of ZooKeeper servers. Throws an
   * IOException on failure.
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  public synchronized void init() throws IOException, InterruptedException,
      KeeperException {
    Preconditions.checkState(this.zk == null, "zk not null in ZKClient.init");

    zk = new ZooKeeper(zkhosts, 5000, this);

    Stat s = zk.exists(znodeBase, true);
    // if (s == null) {
    byte[] data = zk.getData("logicalNodes", this, s);
    List<ACL> acls = null;
    String lnPath = zk.create("logicalNodes", new byte[0], acls,
        CreateMode.PERSISTENT);

    // If has logical node configs, write them
    zk.create(znodeBase, new byte[0], acls, CreateMode.PERSISTENT);
    zk.create(logicalZNode(), new byte[0], acls, CreateMode.PERSISTENT);
    zk.create(logicalSinkZNode(), new byte[0], acls,
        CreateMode.PERSISTENT_SEQUENTIAL);
    zk.create(logicalSourceZNode(), new byte[0], acls,
        CreateMode.PERSISTENT_SEQUENTIAL);
    zk.create(logicalHttpZNode(), data, acls, CreateMode.PERSISTENT);
    zk.create(logicalJsonZNode(), new byte[0], acls, CreateMode.PERSISTENT);
  }

  /**
   * Closes the connection to the ZooKeeper service
   */
  public synchronized void close() throws InterruptedException {
    if (this.zk != null) {
      this.zk.close();
      this.zk = null;
    }
  }

  public void enqueueCheckConfig(LogicalNode ln, FlumeConfigData data) {

  }

  public void dequeueCheckConfig() throws InterruptedException {
  }

  /**
   * Create a liveness manager with the specified managers.
   * 
   * LogicalNodeManager is necessary for tracking physical/logical node
   * mappings. MasterRPC is the connection to the master, WALCompletionNotifier
   * is necessary for check on acks
   */
  public ZKLivenessManager(LogicalNodeManager nodesman, MasterRPC master,
      WALCompletionNotifier walman) {
  }

  /**
   * Checks against the master to get new physical nodes or to learn about
   * decommissioned logical nodes
   * 
   * Invariant: There is always at least logical per physical node. When there
   * is one, it has the same name as the physical node.
   */
  public void checkLogicalNodes() throws IOException, InterruptedException {
  }

  /**
   * Checks registered nodes to see if they need a new configuraiton.
   */
  public void checkLogicalNodeConfigs() throws IOException {
  }

  /**
   * All the core functionality of a heartbeat accessible without having to be
   * in the heartbeat thread.
   */
  public void heartbeatChecks() throws IOException, InterruptedException {
  }

  /**
   * This thread takes checkConfig commands form the q and processes them. We
   * purposely want to decouple the heartbeat from this thread.
   */
  class CheckConfigThread extends Thread {
    CheckConfigThread() {
      super("Check config");
    }

    public void run() {
      try {
        while (!interrupted()) {
          dequeueCheckConfig();
        }
      } catch (InterruptedException ie) {
        LOG.info("Closing");
      }
    }
  };

  /**
   * This thread periodically contacts the master with a heartbeat.
   */
  class HeartbeatThread extends Thread {
    volatile boolean done = false;
    long backoff = BACKOFF_MILLIS;
    long backoffLimit = FlumeConfiguration.get().getNodeHeartbeatBackoffLimit();
    long heartbeatPeriod = FlumeConfiguration.get().getConfigHeartbeatPeriod();
    CountDownLatch stopped = new CountDownLatch(1);

    HeartbeatThread() {
      super("Heartbeat");
    }

    public void run() {
    }

  };

  /**
   * Starts the heartbeat thread and then returns.
   */
  public void start() {
  }

  public void stop() {
  }

  public WALAckManager getAckChecker() {
    return null;
  }

  public int getCheckConfigPending() {
    return 0;
  }

}
