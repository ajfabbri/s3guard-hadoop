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

/**
 * @see FileStatusCacheManager
 * This class holds FileStatus instance with timer associated with instance.
 * Expiry time is configured as part
 * of the Core-site.xml.
 */
final class FileStatusCacheObject {
  private FileStatus fileStatus;
  private long recordedTime;
  private int expireInSeconds;

  /**
   * Constructor.
   *
   * @param fileStatusObject valid FileStatus object
   * @param duration         Expiration period for stored instance in seconds
   */
  FileStatusCacheObject(FileStatus fileStatusObject, int duration) {
    this.fileStatus = fileStatusObject;
    recordedTime = System.nanoTime();
    expireInSeconds = duration;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getRecordedTime() {
    return recordedTime;
  }

  public int getExpireInSeconds() {
    return expireInSeconds;
  }

}
