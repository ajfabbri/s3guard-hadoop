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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Support for the debugging environment during development. Controlled
 * thorough internal flag so the production bit
 * does not have logging enabled jar. it is intentional that flag is set
 * during development only so that production jar
 * does not ship with log enabled jar.
 */
public final class ADLLogger {

  // TODO : Replacing slf4j log
  private static final Log LOG = LogFactory
      .getLog(PrivateAzureDataLakeFileSystem.class);
  private static boolean logEnable = false;
  private static boolean perfEnable = false;

  /**
   * Constructor.
   */
  private ADLLogger() {
  }

  protected static boolean isLogEnabled() {
    return logEnable;
  }

  protected static void setLogEnable(boolean le) {
    logEnable = le;
  }

  protected static boolean isPerfEnabled() {
    return perfEnable;
  }

  protected static void setPerfEnable(boolean enable) {
    perfEnable = enable;
  }

  /**
   * Dump debug information.
   *
   * @param log Log line
   */
  public static void log(String log) {
    if (isLogEnabled()) {
      LOG.info(log);
    }
  }

  /**
   * Dump performance related information. Provided for convenience during
   * development.
   *
   * @param log performance information.
   */
  public static void logPerf(String log) {
    if (isPerfEnabled()) {
      LOG.info(log);
    }
  }
}
