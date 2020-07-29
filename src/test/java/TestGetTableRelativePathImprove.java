import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

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


/**
 *
 * This is test for HFileOutputFormat2 improve case
 * SEE  https://issues.apache.org/jira/browse/HBASE-24791
 *  @author Yechao Chen
 */
public class TestGetTableRelativePathImprove {

    private final static int CELLS = 1000 * 10000;
    private final static String TEST_TABLE_NAME = "default:TestTable";


    private Path getTableRelativePath(byte[] tableNameBytes) {
        String tableName = Bytes.toString(tableNameBytes);
        String[] tableNameParts = tableName.split(":");
        Path tableRelPath = new Path(tableName.split(":")[0]);
        if (tableNameParts.length > 1) {
            tableRelPath = new Path(tableRelPath, tableName.split(":")[1]);
        }
        return tableRelPath;
    }


    /**
     * return the time cost with exec getTableRelativePath
     *
     * @return
     */
    public long before() {
        long start = System.nanoTime();

        /**
         * count means nothing just make the loop do something
         */
        int count = 0;
        for (int i = 0; i < CELLS; i++) {
            byte[] tableNameBytes = Bytes.toBytes("default:TestTable");
            String tableName = Bytes.toString(tableNameBytes);
            count++;
            getTableRelativePath(tableNameBytes);
        }
        return System.nanoTime() - start;
    }

    /**
     * return the time cost without exec getTableRelativePath
     *
     * @return
     */
    public long after() {
        long start = System.nanoTime();
        byte[] tableNameBytes = Bytes.toBytes(TEST_TABLE_NAME);
        int count = 0;
        /**
         * count means nothing just make the loop do something
         */
        for (int i = 0; i < CELLS; i++) {
            count++;
        }
        return System.nanoTime() - start;
    }

    public static void main(String[] args) {
        TestGetTableRelativePathImprove test = new TestGetTableRelativePathImprove();
        StringBuilder beforeCostInfo = new StringBuilder("before improve cost: ");
        StringBuilder afterCostInfo = new StringBuilder("after improve cost: ");
        long beforeCostSumCount = 0L;
        long afterCostSumCount = 0L;

        for (int i = 0; i < 10; i++) {
            long beforeCost = test.before();
            long afterCost = test.after();
            beforeCostSumCount += beforeCost;
            afterCostSumCount += afterCost;

            beforeCostInfo.append(beforeCost).append("\t");
            afterCostInfo.append(afterCost).append("\t");
        }
        System.out.println(beforeCostInfo.toString());
        System.out.println(afterCostInfo.toString());
        System.out.println("before all cost: " + beforeCostSumCount / (1000 * 1000D) +" ms");
        System.out.println("after all cost : " + afterCostSumCount / (1000 * 1000D) +" ms");
        System.out.println("before vs after: " + beforeCostSumCount / (afterCostSumCount * 1.0D)+" : 1");


    }


}
