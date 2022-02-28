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
package github.com.utf7.hdfs.client.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;

/**
 * @author Yechao Chen
 */
public class HdfsDemo {

  private static Logger LOG = LoggerFactory.getLogger(HdfsDemo.class);
  private static final Path TEST_PATH = new Path("/tmp/test2.txt");
  private static final String HDFS_URL = "hdfs://testnn:8020";

  public static void main(String[] args)
      throws IOException, URISyntaxException, InterruptedException {
    FileSystem fs = getFileSystem();
    create(fs, TEST_PATH);
    readInfo(fs, TEST_PATH);
    read(fs, TEST_PATH);

  }

  public static FileSystem getFileSystem()
      throws IOException, URISyntaxException, InterruptedException {

    LOG.info("start run hdfs filesystem example ");
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.setInt("dfs.replication", 3);
    FileSystem fs = FileSystem.get(new URI(HDFS_URL), conf, "hdfs");
    LOG.info("finish get filesystem example");

    return fs;
  }

  public static void create(FileSystem fs, Path path) throws IOException {
    if (fs.exists(path)) {
      LOG.info(" path exists {}", path);
    } else {
      LOG.info("path not exists {} and create", path);
      FSDataOutputStream outputStream = fs.create(path, false);
      for (int i = 1; i <= 100; i++) {
        outputStream.writeUTF("line " + i + "\n");
      }
    }
  }

  public static void readInfo(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      LOG.error(" path not exists {}", path);
    } else {
      FileStatus fileStatus = fs.getFileStatus(path);
      LOG.info(
          "group {} ,owner {}, len {} ,accessTime {},modifyTime {},blockSize {} ,replication {}",
          fileStatus.getGroup(), fileStatus.getOwner(), fileStatus.getLen(),
          new Date(fileStatus.getAccessTime()), new Date(fileStatus.getModificationTime()),
          fileStatus.getBlockSize(), fileStatus.getReplication());

    }
  }

  public static void read(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      LOG.error(" path not exists {}", path);
    } else {
      FSDataInputStream fsDataInputStream = fs.open(path);
      int len = -1;
      //FIXME  if line len > 128,will error,just a example not good case
      byte[] bytes = new byte[128];
      while ((len = fsDataInputStream.read(bytes)) != -1) {
        LOG.info(new String(bytes, 0, len));
      }

    }
  }

}
