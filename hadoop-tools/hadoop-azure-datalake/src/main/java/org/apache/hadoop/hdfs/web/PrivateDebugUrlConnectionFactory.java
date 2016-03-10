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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2ConnectionConfigurator;
import org.apache.hadoop.hdfs.web.resources.ADLHeaderParam;
import org.apache.hadoop.hdfs.web.resources.ADLTrackingClusterNameParam;
import org.apache.hadoop.hdfs.web.resources.ADLTrackingJobIdParam;
import org.apache.hadoop.hdfs.web.resources.ADLTrackingSourceParam;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 * Existing UrlConnectionFactory does not allow url to be configured or
 * monitor connection in detail. This extension
 * provide a way to configure @see PrivateDebugUrlConnection instance which
 * can be used to common configuration like
 * tracking header in the request.
 */
public final class PrivateDebugUrlConnectionFactory
    extends URLConnectionFactory {
  private String objectGuid;
  private Configuration conf;

  PrivateDebugUrlConnectionFactory(
      OAuth2ConnectionConfigurator connConfigurator, String objectGuid,
      Configuration conf) {
    super(connConfigurator);
    this.objectGuid = objectGuid;
    this.conf = conf;
  }

  @Override
  public URLConnection openConnection(URL url, boolean isSpnego)
      throws IOException, AuthenticationException {
    long startTime = System.currentTimeMillis();
    HttpURLConnection urlConnection = (HttpURLConnection) super
        .openConnection(url, isSpnego);
    long endTime = System.currentTimeMillis();

    // Override connection for debugging purpose. Just a hook to get
    // request/response parts
    PrivateDebugUrlConnection connection = new PrivateDebugUrlConnection(url,
        objectGuid, startTime, endTime);
    connection.setAnotherUrlConnection(urlConnection);
    addTrackingHeaders(connection);
    return connection;
  }

  private void addTrackingHeaders(HttpURLConnection urlConnection) {
    String source = conf.get(ADLConfKeys.ADL_EVENTS_TRACKING_SOURCE);
    String clustername = conf.get(ADLConfKeys.ADL_EVENTS_TRACKING_CLUSTERNAME);
    String jobid = conf.get(ADLConfKeys.ADL_TRACKING_JOB_ID);

    ADLTrackingSourceParam sourceParam = new ADLTrackingSourceParam(source);
    ADLTrackingClusterNameParam clusterNameParam = new
        ADLTrackingClusterNameParam(
        clustername);
    ADLTrackingJobIdParam jobidParam = new ADLTrackingJobIdParam(jobid);

    urlConnection.setRequestProperty("x-ms-tracking-info", ADLHeaderParam
        .toSortedHeaderString(",", sourceParam, clusterNameParam, jobidParam));
  }
}