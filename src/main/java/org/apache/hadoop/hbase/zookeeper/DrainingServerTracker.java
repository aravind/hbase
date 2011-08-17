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
package org.apache.hadoop.hbase.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the list of draining region servers via ZK.
 *
 * <p>This class is responsible for watching for changes to the draining
 * servers list.  It handles adds/deletes in the draining RS list and
 * watches each node.
 *
 * <p>If an RS gets deleted from draining list, we call
 * {@link ServerManager#removeServerFromDrainList(String)}.
 *
 * <p>If an RS gets added to the draining list, we add a watcher to it and call
 * {@link ServerManager#addServerToDrainList(String)}.
 *
 */
public class DrainingServerTracker extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(DrainingServerTracker.class);

  private ServerManager serverManager;
  private Abortable abortable;

  public DrainingServerTracker(ZooKeeperWatcher watcher,
      Abortable abortable, ServerManager serverManager) {
    super(watcher);
    this.abortable = abortable;
    this.serverManager = serverManager;
  }

  /**
   * Starts the tracking of draining RegionServers.
   *
   * <p>All Draining RSs will be tracked after this method is called.
   *
   * @throws KeeperException
   */
  public void start() throws KeeperException {
    watcher.registerListener(this);
    ZKUtil.watchAndGetNewChildren(watcher, watcher.drainingZNode);
  }

  @Override
  public void nodeDeleted(final String path) {
    if(path.startsWith(watcher.drainingZNode)) {
      final String serverName = ZKUtil.getNodeName(path);
      LOG.info("Draining RS node deleted, removing from list [" +
          serverName + "]");
      serverManager.removeServerFromDrainList(serverName);
    }
  }

  @Override
  public void nodeCreated(final String path) {
    if(path.startsWith(watcher.drainingZNode)) {
      final String serverName = ZKUtil.getNodeName(path);
      LOG.info("Draining RS node created, adding it to the list [" +
          serverName + "]");
      serverManager.addServerToDrainList(serverName);
    }
  }

  @Override
  public void nodeChildrenChanged(final String path) {
    if(path.equals(watcher.drainingZNode)) {
      try {
        final List<ZKUtil.NodeAndData> newNodes = ZKUtil.watchAndGetNewChildren(watcher,
                                                         watcher.drainingZNode);
        for (final ZKUtil.NodeAndData nodeData: newNodes) {
          nodeCreated(nodeData.getNode());
        }
      } catch (KeeperException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      }
    }
  }

}
