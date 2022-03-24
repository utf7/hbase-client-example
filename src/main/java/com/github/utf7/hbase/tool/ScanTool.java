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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Region Tools
 * <p>
 * merge adjacent small Regions of a table
 * </p>
 *
 * @author Yechao Chen
 */
public class ScanTool {

  private final static Logger LOG = LoggerFactory.getLogger(ScanTool.class);

  /**
   * merge region when region size less than sizeMb
   *
   * @param connection
   * @param tableName
   * @throws IOException
   */
  public int scan(Connection connection, String tableName, byte[] startKey, int limit)
      throws IOException {
    int rowCount = 0;

    long start = System.currentTimeMillis();
    try (Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner rs = table.getScanner(new Scan().withStartRow(startKey).setLimit(limit))) {
      for (Result r = rs.next(); r != null; r = rs.next()) {
        rowCount++;
        for (Cell kv : r.rawCells()) {
          //ignore
        }
      }
      long cost = System.currentTimeMillis() - start;
      LOG.info("scan table {}  with startKey {} with limit {},result rowCount {} and cost {} ms ",
          tableName, Bytes.toString(startKey), limit, rowCount, cost);
    }

    return rowCount;

  }

  /**
   * @param connection
   * @param tableName
   * @param threads
   * @param total
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void pscan(Connection connection, String tableName, int threads, int total) {
    try {
      long start = System.currentTimeMillis();
      ExecutorService es = Executors.newFixedThreadPool(threads);
      List<HRegionInfo> regionInfos =
          connection.getAdmin().getTableRegions(TableName.valueOf(tableName));
      int regions = regionInfos.size();
      threads = regions > threads ? regions : threads;
      List<Future<Integer>> futures = new ArrayList<>();
      int limit = total / threads;

      for (HRegionInfo regionInfo : regionInfos) {
        Future<Integer> result =
            es.submit(() -> scan(connection, tableName, regionInfo.getStartKey(), limit));
        futures.add(result);
      }
      AtomicInteger count = new AtomicInteger(0);
      for (Future<Integer> f : futures) {
        count.addAndGet(f.get());
      }
      LOG.info("finish scan table {}  threads:{} totals:{},resultCount:{}  cost {} ms,", tableName,
          threads, total, count, System.currentTimeMillis() - start);
    } catch (Exception e) {
      LOG.error("concurrent scan error ", e);
    }

  }

  public static void main(String[] args) {
    String type = args[0];
    try (Connection connection = HbaseConnectionFactory.getConnection()) {
      ScanTool regionTool = new ScanTool();
      String tableName = args[1];
      switch (type) {
        case "scan":
          byte[] startkey = args[2].getBytes(StandardCharsets.UTF_8);
          int limit = Integer.parseInt(args[3]);
          regionTool.scan(connection, tableName, startkey, limit);
        case "pscan":
          int threads = Integer.parseInt(args[2]);
          int total = Integer.parseInt(args[3]);
          regionTool.pscan(connection, tableName, threads, total);
        default:
          LOG.info(
              "help：\n  1. 100 threads/total scan 300000rows , com.github.utf7.hbase.tool.ScanTool  \"pscan\" \"test 100 300000");
      }

    } catch (IOException e) {
      LOG.error("{} region error", type, e);
    }
  }
}
