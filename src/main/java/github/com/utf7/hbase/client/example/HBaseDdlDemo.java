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


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.RegionLocator;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * show how to use HBase DDL with java api
 * <p>
 * DDL is : create table,disable table,drop table,alter table,list table,get region location,compact,split
 *
 * @author Yechao Chen
 */
public class HBaseDdlDemo {


    private static Logger LOG = LoggerFactory.getLogger(HBaseDdlDemo.class);

    private static final int SPLITS = 9;
    private Connection connection;
    private TableName tableName;
    private byte[] columnFamily;

    public HBaseDdlDemo(Connection connection, TableName tableName, byte[] columnFamily) {
        this.connection = connection;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
    }


    public void listTables() throws IOException {
        TableName[] tableNames = connection.getAdmin().listTableNames();
        StringBuilder tables = new StringBuilder();
        for (TableName tableName : tableNames) {
            tables.append(tableName.getNameAsString()).append("\t");
        }
        LOG.info("find {} tables {}:", tableNames.length, tables.toString());
    }

    public void createTableWithSplit() throws IOException {
        LOG.info("start create table {} ", tableName.getNameAsString());
        long start = System.currentTimeMillis();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        byte[][] splitKeys = new byte[SPLITS][];
        for (int i = 1; i <= SPLITS; i++) {
            splitKeys[i - 1] = Bytes.toBytes(i);
        }

        HColumnDescriptor hColumnDescriptor =
                new HColumnDescriptor(columnFamily);
        //also you can set some config for this table/cf like that
        hColumnDescriptor.setBlockCacheEnabled(true);
        hColumnDescriptor.setCompressionType(Compression.Algorithm.ZSTD);
        short dfsReplication = 3;
        hColumnDescriptor.setDFSReplication(dfsReplication);
        hColumnDescriptor.setConfiguration("hbase.hstore.blockingStoreFiles", "30");
        hTableDescriptor.addFamily(hColumnDescriptor);

        connection.getAdmin().createTable(hTableDescriptor, splitKeys);
        LOG.info("success create table  {} and cost {} ms", tableName.getNameAsString(),
                (System.currentTimeMillis() - start));

    }


    public void readRegionLocators() throws IOException {
        StringBuilder regionLocationInfo = new StringBuilder();
        try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
            List<HRegionLocation> regionLocators = regionLocator.getAllRegionLocations();
            for (HRegionLocation hRegionLocation : regionLocators) {
                regionLocationInfo.append(hRegionLocation.toString());
            }
        }
        LOG.info("table {} locators : {} ", tableName.getNameAsString(), regionLocationInfo.toString());
    }


}
