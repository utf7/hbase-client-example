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
package com.github.utf7.hbase.tool;

import com.github.utf7.hbase.client.example.HbaseConnectionFactory;
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

  private void merge(Admin admin, TableName tableName, int sizeMb) throws IOException {

    Map<byte[], RegionLoad> regionLoadMap = getRegionsLoad(admin);

    List<HRegionInfo> regionInfos = admin.getTableRegions(tableName);
    int successMergeCount = 0;
    int failedMergeCount = 0;
    LOG.info("starting merge table {} regions", tableName.getNameWithNamespaceInclAsString());
    for (int i = 0; i < regionInfos.size() - 1; i++) {
      HRegionInfo first = regionInfos.get(i);
      HRegionInfo second = regionInfos.get(i + 1);
      if (regionLoadMap.get(first.getRegionName()).getStorefileSizeMB() < sizeMb
          && regionLoadMap.get(second.getRegionName()).getStorefileSizeMB() < sizeMb) {
        if (HRegionInfo.areAdjacent(first, second)) {
          long firstRegionSizeMb = getRegionSizeMb(regionLoadMap, first);
          long secondRegionSizeMb = getRegionSizeMb(regionLoadMap, second);
          long afterMergeRegionSzieMb = firstRegionSizeMb + secondRegionSizeMb;
          LOG.info("starting merge region : {}:{}Mb with {}:{}Mb  after {} Mb",
              first.getRegionNameAsString(), firstRegionSizeMb, second.getRegionNameAsString(),
              secondRegionSizeMb, afterMergeRegionSzieMb);

          try {
            admin.mergeRegions(first.getEncodedNameAsBytes(), second.getEncodedNameAsBytes(),
                false);
            LOG.info("finish merge region : {}:{}Mb with {}:{}Mb  after {} Mb",
                first.getRegionNameAsString(), firstRegionSizeMb, second.getRegionNameAsString(),
                secondRegionSizeMb, afterMergeRegionSzieMb);

            successMergeCount++;
          } catch (Exception e) {
            LOG.error("merge region failed {},{} error:", first.getRegionNameAsString(),
                second.getRegionNameAsString(), e);
            failedMergeCount++;
          }
        }
      }
    }
    LOG.info("success merge region {} ,failed merge regions {}", successMergeCount,
        failedMergeCount);

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

  /**
   * @param regionLoadMap
   * @param hri
   * @return region size with MB
   */
  private static long getRegionSizeMb(Map<byte[], RegionLoad> regionLoadMap, HRegionInfo hri) {
    RegionLoad regionLoad = regionLoadMap.get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.info(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }

  /**
   * @param hri
   * @return region size with MB
   */
  private long getRegionSizeMb(Admin admin, HRegionInfo hri) throws IOException {
    Map<byte[], RegionLoad> regionLoadMap = getRegionsLoad(admin);
    RegionLoad regionLoad = regionLoadMap.get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.info(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }

  /**
   * merge region when region size less than sizeMb
   *
   * @param admin
   * @param tableName
   * @param sizeMb
   * @throws IOException
   */
  public void merge(Admin admin, String tableName, int sizeMb) throws IOException {
    merge(admin, TableName.valueOf(tableName), sizeMb);

  }

  public void split(Admin admin, String tableName, int sizeMb) throws IOException {
    List<HRegionInfo> regionInfos = admin.getTableRegions(TableName.valueOf(tableName));
    Map<byte[], RegionLoad> regionLoadMap = getRegionsLoad(admin);
    LOG.info("starting split table {} regions with limit <= sizeMb {}", tableName, sizeMb);
    int successSplitCount = 0;
    int failedSplitCount = 0;
    for (HRegionInfo hRegionInfo : regionInfos) {
      long regionSize = getRegionSizeMb(regionLoadMap, hRegionInfo);
      if (regionSize > sizeMb) {
        try {
          admin.split(TableName.valueOf(tableName));
          successSplitCount++;
          LOG.info("finish split {} region : {}:{}Mb", tableName,
              hRegionInfo.getRegionNameAsString(), regionSize);

        } catch (Exception e) {
          failedSplitCount++;
          LOG.error("split table {},region : {} failed error:", tableName,
              hRegionInfo.getRegionNameAsString(), e);
        }
      }
    }

    LOG.info("success split {} regions ,failed split {} regions", successSplitCount,
        failedSplitCount);

  }

  public static void main(String[] args) {
    String type = args[0];
    try (Connection connection = HbaseConnectionFactory.getConnection();
        Admin admin = connection.getAdmin()) {
      RegionTool regionTool = new RegionTool();
      String tableName = args[1];
      switch (type) {
        case "merge":
          int mergeLimitMb = Integer.parseInt(args[2]);
          regionTool.merge(admin, tableName, mergeLimitMb);
          break;
        case "split":
          int splitLimitMb = Integer.parseInt(args[2]);
          regionTool.split(admin, tableName, splitLimitMb);
          break;
        default:
          LOG.info("help====");
      }

    } catch (IOException e) {
      LOG.error("{} region error", type, e);
    }
  }
}
