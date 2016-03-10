/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class TestFileStatusCacheManager {

  Random r = new Random();
  int Low = 500;
  int High = 2000;

  @Test
  public void testCache() {
    FileStatusCacheManager fileStatusCacheManager = FileStatusCacheManager
        .getInstance();

    Path path = new Path("/abc/xyz");
    FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);

    fileStatusCacheManager.put(status, 5);
    FileStatus actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Assert.assertNotEquals(actual.getPath().toString(), "/abc");
    fileStatusCacheManager.remove(path);
    actual = fileStatusCacheManager.get(path);
    Assert.assertNull(actual);
  }

  @Test
  public void testCacheExpiryTime() throws InterruptedException {
    FileStatusCacheManager fileStatusCacheManager = FileStatusCacheManager
        .getInstance();

    Path path = new Path("/abc/xyz");
    FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);

    fileStatusCacheManager.put(status, 1);
    FileStatus actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Thread.sleep(100);
    actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Thread.sleep(200);
    actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Thread.sleep(100);
    actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Thread.sleep(500);
    actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Thread.sleep(200);
    actual = fileStatusCacheManager.get(path);
    Assert.assertNull(actual);
  }

  @Test
  public void testCacheSizeLimit() throws InterruptedException {
    FileStatusCacheManager fileStatusCacheManager = FileStatusCacheManager
        .getInstance();

    for (int i = 0; i < 5010; ++i) {
      Path path = new Path("/abc/xyz" + i);
      FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);
      fileStatusCacheManager.put(status, 1);
    }
    Assert.assertEquals(fileStatusCacheManager.getCacheSize(), 5000);

    for (int i = 0; i < 1000; ++i) {
      Path path = new Path("/abc/xyz" + i);
      FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);
      fileStatusCacheManager.put(status, 1);
    }

    Assert.assertEquals(fileStatusCacheManager.getCacheSize(), 5000);
    fileStatusCacheManager.clear();
  }

  @Test
  public void testResubmitEntry() {
    FileStatusCacheManager fileStatusCacheManager = FileStatusCacheManager
        .getInstance();
    fileStatusCacheManager.clear();
    Path path = new Path("/abc/xyz");
    FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);
    fileStatusCacheManager.put(status, 1);
    fileStatusCacheManager.put(status, 1);
    fileStatusCacheManager.put(status, 1);
    Assert.assertEquals(fileStatusCacheManager.getCacheSize(), 1);
    FileStatus actual = fileStatusCacheManager.get(path);
    Assert
        .assertEquals(actual.getPath().toString(), status.getPath().toString());
    Assert.assertNotEquals(actual.getPath().toString(), "/abc");
  }

  @Test
  public void testConcurrentEntries() throws InterruptedException {
    FileStatusCacheManager.getInstance().clear();
    int testCount = 100;
    Thread[] threadPool = new Thread[testCount];
    String[] uniqueFileName = new String[testCount * 10];

    for (int i = 0; i < (testCount * 10); ++i) {
      uniqueFileName[i] = UUID.randomUUID().toString();
    }

    for (int i = 0; i < testCount; ++i) {
      threadPool[i] = new CacheManagerTest(uniqueFileName);
    }

    for (int i = 0; i < testCount; ++i) {
      threadPool[i].start();
    }

    for (int i = 0; i < testCount; ++i) {
      threadPool[i].join();
    }
  }

  class CacheManagerTest extends Thread {
    String[] uniqueFileName;

    public CacheManagerTest(String[] uniqueFileName) {
      this.uniqueFileName = uniqueFileName;
    }

    public void run() {
      FileStatusCacheManager fileStatusCacheManager = FileStatusCacheManager
          .getInstance();

      for (int i = 0; i < uniqueFileName.length; i++) {
        Path path = new Path("/abc/xyz/" + uniqueFileName[i]);
        fileStatusCacheManager.remove(path);
        Assert.assertNull(fileStatusCacheManager.get(path));
      }

      try {
        Thread.sleep(r.nextInt(High - Low) + Low);
      } catch (InterruptedException e) {
      }

      for (int i = 0; i < uniqueFileName.length; i++) {
        Path path = new Path("/abc/xyz/" + uniqueFileName[i]);
        FileStatus status = new FileStatus(100, true, 1, 4, 123414, path);
        fileStatusCacheManager.put(status, 10);
        FileStatus actual = fileStatusCacheManager.get(path);
        if (actual == null) {
          System.out.println("Path " + path.toString());
        }
        Assert.assertEquals(actual.getPath().toString(),
            status.getPath().toString());
        Assert.assertNotEquals(actual.getPath().toString(), "/abc");
      }
    }
  }
}
