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
package github.com.utf7.hbase.tool;

import github.com.utf7.hbase.client.example.HbaseConnectionFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Region Tools
 * <p>
 * merge adjacent small Regions of a table
 * </p>
 *
 * @author Yechao Chen
 */
public class RegionTool {

  private final static Logger LOG = LoggerFactory.getLogger(RegionTool.class);

  private void merge(Connection connection, TableName tableName, int sizeMb) throws IOException {

    try (Admin admin = connection.getAdmin()) {
      Map<byte[], RegionLoad> regionLoadMap = getRegionsLoad(admin);

      List<HRegionInfo> regionInfos = admin.getTableRegions(tableName);
      for (int i = 0; i < regionInfos.size() - 1; i++) {
        HRegionInfo first = regionInfos.get(i);
        HRegionInfo second = regionInfos.get(i + 1);

        int successMergeCount = 0;
        int failedMergeCount = 0;

        if (regionLoadMap.get(first.getRegionName()).getStorefileSizeMB() < sizeMb
            && regionLoadMap.get(second.getRegionName()).getStorefileSizeMB() < sizeMb) {
          if (HRegionInfo.areAdjacent(first, second)) {
            LOG.info("merge region : {}:{}Mb with {}:{}Mb ", first.getRegionNameAsString(),
                getRegionSize(regionLoadMap, first), second.getRegionNameAsString(),
                getRegionSize(regionLoadMap, second));
            try {
              admin.mergeRegions(first.getEncodedNameAsBytes(), second.getEncodedNameAsBytes(),
                  false);
              successMergeCount++;
            } catch (Exception e) {
              failedMergeCount++;
            }
          }
        }
        LOG.info("success merge region {} ,failed merge regions {}", successMergeCount,
            failedMergeCount);
      }
    }

  }

  private Map<byte[], RegionLoad> getRegionsLoad(Admin admin) throws IOException {
    Map<byte[], RegionLoad> regionLoads = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    Collection<ServerName> serverNames = admin.getClusterStatus().getServers();
    for (ServerName sn : serverNames) {
      Map<byte[], RegionLoad> regionLoadMap = admin.getClusterStatus().getLoad(sn).getRegionsLoad();
      regionLoads.putAll(regionLoadMap);
    }
    return regionLoads;
  }

  private static long getRegionSize(Map<byte[], RegionLoad> regionLoadMap, HRegionInfo hri) {
    RegionLoad regionLoad = regionLoadMap.get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.info(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }

  public void merge(Connection connection, String tableName, int sizeMb) throws IOException {
    merge(connection, TableName.valueOf(tableName), sizeMb);

  }

  public static void main(String[] args) {
    try (Connection connection = HbaseConnectionFactory.getConnection()) {
      RegionTool regionTool = new RegionTool();
      String tableName = args[0];
      int sizeMb = Integer.parseInt(args[1]);
      regionTool.merge(connection, tableName, sizeMb);
    } catch (IOException e) {
      LOG.error("merge region error", e);
    }
  }
}
