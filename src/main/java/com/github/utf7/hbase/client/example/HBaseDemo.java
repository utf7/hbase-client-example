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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Yechao Chen
 */
public class HBaseDemo {

    private static Logger LOG = LoggerFactory.getLogger(HBaseDemo.class);

    private static final TableName TEST_TABLE = TableName.valueOf("test");
    private static final byte[] CF = Bytes.toBytes("cf");


    public static void main(String[] args) {
        try (Connection connection = HbaseConnectionFactory.getConnection()) {
            execDdl(connection);
            execDml(connection);
        } catch (IOException e) {
            LOG.error("exec hbase client example error ", e);
        }

    }

    public static void execDdl(Connection connection) throws IOException {
        LOG.info("start run ddl client example ");
        HBaseDdlDemo ddlDemo = new HBaseDdlDemo(connection, TEST_TABLE, CF);
        ddlDemo.listTables();
        ddlDemo.createTableWithSplit();
        ddlDemo.listTables();
        ddlDemo.readRegionLocators();
        LOG.info("finish run ddl client example");
    }

    public static void execDml(Connection connection) throws IOException {
        HBaseDmlDemo dmlDemo = new HBaseDmlDemo(connection, TEST_TABLE, CF);
        dmlDemo.put();
        dmlDemo.batchPuts();
        dmlDemo.get();
        dmlDemo.scan();
    }


}
