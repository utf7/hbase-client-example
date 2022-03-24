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
package com.github.utf7.hbase.client.example;

import com.github.utf7.hbase.client.example.util.DataUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * show how to use hbase dml with java api
 * <p>
 * dml is : put,batch put ,get,scan ,delete,increment
 *
 * @author Yechao Chen
 */
public class HBaseDmlDemo {

  private static Logger LOG = LoggerFactory.getLogger(HBaseDmlDemo.class);

  private static final int SPLITS = 9;
  private static int ROWS = 10000;

  private static final int COLUMNS = 10;

  private static final int BATCH = 100;
  private static int VALUE_SIZE = 128;
  private static final Random RADDOM_REED = new Random(System.currentTimeMillis());

  private static long nextRandomSeed() {
    return RADDOM_REED.nextLong();
  }

  private final static Random RANDOM = new Random(nextRandomSeed());

  private Connection connection;
  private TableName tableName;
  private byte[] columnFamily;

  public HBaseDmlDemo(Connection connection, TableName tableName, byte[] columnFamily) {
    this.connection = connection;
    this.tableName = tableName;
    this.columnFamily = columnFamily;
  }

  public int put() throws IOException {
    long start = System.currentTimeMillis();
    LOG.info("start put a row  to table {}", tableName.getNameAsString());
    try (Table table = connection.getTable(tableName)) {
      Put put = new Put(Bytes.toBytes( RandomUtils.nextInt(10) + "-row-" + UUID.randomUUID().toString()));
      for (int cIndex = 0; cIndex < COLUMNS; cIndex++) {
        put.addColumn(columnFamily, Bytes.toBytes("c" + cIndex),
            DataUtil.generateData(RANDOM, VALUE_SIZE));
      }
      table.put(put);
    }

    LOG.info("finish put a row to table {} and cost {} ms", tableName.getNameAsString(),
        (System.currentTimeMillis() - start));
    return ROWS;
  }

  public int batchPuts() throws IOException {
    long start = System.currentTimeMillis();

    LOG.info(" start batch put data to table {} rows: {}", tableName.getNameAsString(), ROWS);

    try (Table table = connection.getTable(tableName)) {
      List<Put> puts = new ArrayList<>(BATCH);
      for (int i = 0; i < ROWS; i++) {
        Put put = new Put(DataUtil.generateRowkey(0, SPLITS + 1, i));
        for (int cIndex = 0; cIndex < COLUMNS; cIndex++) {
          put.addColumn(columnFamily, Bytes.toBytes("c" + cIndex),
              DataUtil.generateData(RANDOM, VALUE_SIZE));
        }
        puts.add(put);
        if (puts.size() % BATCH == 0) {
          table.put(puts);
          puts.clear();
          puts = new ArrayList<>(BATCH);
        }

      }
      if (CollectionUtils.isNotEmpty(puts)) {
        table.put(puts);
        puts.clear();
      }
    }
    LOG.info("finish batch put data to table {}  rows {} and cost {} ms",
        tableName.getNameAsString(), ROWS, (System.currentTimeMillis() - start));

    return ROWS;
  }

  public void scan() throws IOException {
    LOG.info("start scan from table {}", tableName.getNameAsString());
    long start = System.currentTimeMillis();

    int rowCount = 0;
    Scan scan = new Scan();
    try (Table table = connection.getTable(tableName); ResultScanner rs = table.getScanner(scan)) {
      StringBuilder rowsInfos = new StringBuilder();
      for (Result r = rs.next(); r != null; r = rs.next()) {
        rowCount++;
        for (Cell kv : r.rawCells()) {
          rowsInfos.append(DataUtil.convert2PirntString(kv));
          if (rowCount % BATCH == 0) {
            LOG.info(rowsInfos.toString());
            rowsInfos = new StringBuilder();
          }
        }
      }
      if (rowsInfos.length() > 0) {
        LOG.info(rowsInfos.toString());
      }

    }

    LOG.info("finish scan table {} ,rows: {} ,cost {} ms " + tableName.getNameAsString(), rowCount,
        (System.currentTimeMillis() - start));

  }

  public void get() throws IOException {
    LOG.info("start scan from table {}", tableName.getNameAsString());
    long start = System.currentTimeMillis();

    int getTimes = 100;
    try (Table table = connection.getTable(tableName)) {
      for (int i = 0; i < getTimes; i++) {
        Get get = new Get(DataUtil.generateRowkey(0, SPLITS + 1, RandomUtils.nextInt(ROWS)));
        Result result = table.get(get);
        CellScanner cellScanner = result.cellScanner();
        StringBuilder rowsInfos = new StringBuilder();
        while (!result.isEmpty() && cellScanner.advance()) {
          Cell currentCell = result.current();
          rowsInfos.append(DataUtil.convert2PirntString(currentCell));
        }
        LOG.info(rowsInfos.toString());
      }

      LOG.info("finish get data table {} ,cost {} ms " + tableName.getNameAsString(),
          (System.currentTimeMillis() - start));
    }

  }

}
