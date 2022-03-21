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

import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Yechao Chen
 */
public class StopWatchTest {

  private static final Logger LOG = LoggerFactory.getLogger(StopWatchTest.class);

  public static void main(String[] args) throws InterruptedException {
    StopWatch sw = new StopWatch();
    sw.reset().start();
    Thread.sleep(2*1000L);
    LOG.info("cost {} ms ",sw.now(TimeUnit.MILLISECONDS));
    sw.reset().start();
    Thread.sleep(3*1000L);
    LOG.info("cost {} ms ",sw.now(TimeUnit.MILLISECONDS));
    sw.reset().start();
    Thread.sleep(4*1000L);
    LOG.info("cost {} ms ",sw.now(TimeUnit.MILLISECONDS));
    sw.stop();

    for(int i=1;i<=100;i++){
      int remainder = i %10;
      if(remainder ==0){
        System.out.println(i+"reached :"+remainder);
      }else{
        System.out.println(i+"not reached: "+remainder);
      }
    }


  }
}
