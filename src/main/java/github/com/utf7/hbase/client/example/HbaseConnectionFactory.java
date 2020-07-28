/**
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
package github.com.utf7.hbase.client.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author chenyechao
 */
public class HbaseConnectionFactory {
    private static Logger LOG = LoggerFactory.getLogger(HBaseDemo.class);

    private final static Object LOCK = new Object();
    private static volatile Connection conn;
    private final static String ZK_QUORUM = "zk1,zk2,zk3";
    private final static String ZK_ZNODE_PRAENT = "/hbase";
    private final static int ZK_CLIENT_PORT = HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT;

    public static Connection getConnection() throws IOException {
        if (conn == null) {
            synchronized (LOCK) {
                if (conn == null) {
                    Configuration hbaseConf = HBaseConfiguration.create();
                    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, ZK_QUORUM);
                    hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ZK_ZNODE_PRAENT);
                    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, ZK_CLIENT_PORT);

                    // for fast test
                    hbaseConf.set("hbase.client.retries.number", "5");

                    LOG.info(HConstants.ZOOKEEPER_QUORUM + ": {}",
                            hbaseConf.get(HConstants.ZOOKEEPER_QUORUM));
                    LOG.info(HConstants.ZOOKEEPER_CLIENT_PORT + ": {}",
                            hbaseConf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
                    LOG.info(HConstants.ZOOKEEPER_ZNODE_PARENT + ": {}",
                            hbaseConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
                    conn = ConnectionFactory.createConnection(hbaseConf);
                }
            }
        }
        return conn;

    }
}
