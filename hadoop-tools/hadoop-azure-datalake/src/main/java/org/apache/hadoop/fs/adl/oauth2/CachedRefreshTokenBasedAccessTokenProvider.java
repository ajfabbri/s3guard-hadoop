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

package org.apache.hadoop.fs.adl.oauth2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.oauth2
    .ConfRefreshTokenBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2
    .PrivateCachedRefreshTokenBasedAccessTokenProvider;
import org.apache.hadoop.util.Timer;

import java.io.IOException;

/**
 * Extension to AccessTokenProvider to maintain token information per process
 * level. Share access token across multiple
 * instances of AdlFileSystem, Allows performance improvement since multiple
 * AAD calls are avoided within the process.
 * TODO : Document how to use this class
 */
public final class CachedRefreshTokenBasedAccessTokenProvider
    extends PrivateCachedRefreshTokenBasedAccessTokenProvider {
  private static ConfRefreshTokenBasedAccessTokenProvider instance = null;

  /**
   * Constructor.
   */
  public CachedRefreshTokenBasedAccessTokenProvider() {
    if (instance == null) {
      instance = new ConfRefreshTokenBasedAccessTokenProvider();
    }
  }

  /**
   * Constructor.
   *
   * @param time timer instance from the time last access token was issued.
   */
  public CachedRefreshTokenBasedAccessTokenProvider(Timer time) {
    if (instance == null) {
      instance = new ConfRefreshTokenBasedAccessTokenProvider(time);
    }
  }

  /**
   * Gets the access token from internally cached
   * ConfRefreshTokenBasedAccessTokenProvider instance.
   *
   * @return Valid OAuth2 access token for the user.
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public String getAccessToken() throws IOException {
    return instance.getAccessToken();
  }

  /**
   * @return Return configuration instance
   */
  @Override
  public Configuration getConf() {
    return instance.getConf();
  }

  /**
   * Set configuration instance.
   *
   * @param conf Configuration instance
   */
  @Override
  public void setConf(Configuration conf) {
    instance.setConf(conf);
    try {
      this.getAccessToken();
    } catch (IOException e) {
      //TODO : Add printing logs here
    }
  }
}
