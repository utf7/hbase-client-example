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
package com.github.utf7.tool;

import com.github.utf7.hbase.client.example.HbaseConnectionFactory;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
      LOG.info("starting merge table {} regions", tableName.getNameWithNamespaceInclAsString());

      Map<byte[], RegionMetrics> regionLoadMap = getRegionsLoad(admin);
      List<RegionInfo> regionInfos = admin.getRegions(tableName);

      int successMergeCount = 0;
      int failedMergeCount = 0;

      for (int i = 0; i < regionInfos.size() - 1; i++) {
        RegionInfo first = regionInfos.get(i);
        RegionInfo second = regionInfos.get(i + 1);
        double firstRegionSizeMb = getRegionSizeMb(regionLoadMap, first);
        double secondRegionSizeMb = getRegionSizeMb(regionLoadMap, second);
        if (regionLoadMap.get(first.getRegionName()).getStoreFileSize().get(Size.Unit.MEGABYTE)
            < sizeMb
            && regionLoadMap.get(second.getRegionName()).getStoreFileSize().get(Size.Unit.MEGABYTE)
            < sizeMb) {
          if (RegionInfo.areAdjacent(first, second)) {
            LOG.info("starting merge region : {}:{}Mb with {}:{}Mb, after: {} Mb ",
                first.getRegionNameAsString(), firstRegionSizeMb, second.getRegionNameAsString(),
                secondRegionSizeMb, firstRegionSizeMb + secondRegionSizeMb);
            byte[][] nameofRegionsToMerge = new byte[2][];
            nameofRegionsToMerge[0] = first.getEncodedNameAsBytes();
            nameofRegionsToMerge[1] = second.getEncodedNameAsBytes();
            try {
              admin.mergeRegionsAsync(nameofRegionsToMerge, false);
              LOG.info("finish merge region : {}:{}Mb with {}:{}Mb  after {} Mb",
                  first.getRegionNameAsString(), firstRegionSizeMb, second.getRegionNameAsString(),
                  secondRegionSizeMb, firstRegionSizeMb + secondRegionSizeMb);
              successMergeCount++;
            } catch (Exception e) {
              LOG.error("merge region failed {},{} error:", first.getRegionNameAsString(),
                  second.getRegionNameAsString(), e);
              failedMergeCount++;
            }
          }
        }
      }
      LOG.info("success merge pair regions  {} ,failed merge pair regions {}", successMergeCount,
          failedMergeCount);
    }

  }

  private Map<byte[], RegionMetrics> getRegionsLoad(Admin admin) throws IOException {
    Map<byte[], RegionMetrics> regionLoads = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    Map<ServerName, ServerMetrics> liveServerMetrics =
        admin.getClusterMetrics().getLiveServerMetrics();
    liveServerMetrics.values().forEach(s -> regionLoads.putAll(s.getRegionMetrics()));
    return regionLoads;
  }

  private static double getRegionSizeMb(Map<byte[], RegionMetrics> regionLoadMap, RegionInfo hri) {
    RegionMetrics regionLoad = regionLoadMap.get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.info(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE);
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