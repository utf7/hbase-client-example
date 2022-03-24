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
package com.github.utf7.hbase.client.example.util;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

/**
 * @author Yechao Chen
 */
public class DataUtil {

  public static byte[] generateRowkey(int startPrefix, int endPrefix, int index) {
    return Bytes.toBytes(RandomUtils.nextInt(endPrefix) + startPrefix + "-" + index);
  }

  public static byte[] generateData(final Random r, int length) {
    byte[] b = new byte[length];
    int i;

    int bits = 8;
    for (i = 0; i < (length - bits); i += bits) {
      b[i] = (byte) (65 + r.nextInt(26));
      b[i + 1] = b[i];
      b[i + 2] = b[i];
      b[i + 3] = b[i];
      b[i + 4] = b[i];
      b[i + 5] = b[i];
      b[i + 6] = b[i];
      b[i + 7] = b[i];
    }

    byte a = (byte) (65 + r.nextInt(26));
    for (; i < length; i++) {
      b[i] = a;
    }
    return b;
  }

  public static String convert2PirntString(Cell kv) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("rowkey=").append(Bytes.toString(kv.getRowArray())).append(",");
    stringBuilder.append(Bytes.toString(kv.getFamilyArray())).append(":")
        .append(kv.getQualifierArray()).append("=");
    stringBuilder.append(Bytes.toString(kv.getValueArray()));
    stringBuilder.append("\n");
    return stringBuilder.toString();

  }

}
