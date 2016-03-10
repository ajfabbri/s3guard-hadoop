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

import org.apache.hadoop.fs.adl.TestADLResponseData;
import org.apache.hadoop.fs.adl.TestableAdlFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.TestBaseSetup;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.PrivateAzureDataLakeFileSystem
    .BatchByteArrayInputStream;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.Parameter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestConcurrentRead extends TestBaseSetup {

  @Test
  public void testSplitSizeCalculations()
      throws URISyntaxException, IOException {
    MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost",
        WEBHDFS_PORT);
    MockServerClient mockOAuthServerClient = new MockServerClient("localhost",
        OAUTH_PORT);

    HttpRequest oauthServerRequest = getOAuthServerMockRequest(
        mockOAuthServerClient);

    HttpRequest getFileStatusRequest = request().withMethod("GET")
        .withPath("/webhdfs/v1/test1/test2")
        .withQueryStringParameter(new Parameter("op", "GETFILESTATUS"))
        .withHeader(AUTH_TOKEN_HEADER);

    try {
      mockWebHDFSServerClient.when(getFileStatusRequest, exactly(1)).respond(
          response().withStatusCode(HttpStatus.SC_OK).withBody(
              TestADLResponseData
                  .getGetFileStatusJSONResponse(128 * 1024 * 1024)));

      TestableAdlFileSystem fs = new TestableAdlFileSystem();
      Configuration conf = getADLConfiguration();
      conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" +
          OAUTH_PORT + "/refresh");
      conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
          "credential");

      URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
      URL url = new URL("http://localhost");
      fs.initialize(uri, conf);

      BatchByteArrayInputStream stream = fs.new BatchByteArrayInputStream(url,
          new Path("/test1/test2"), 16 * 1024 * 1024, 4);
      Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
      Assert.assertEquals(4, stream.getSplitSize(16 * 1024 * 1024));
      Assert.assertEquals(3, stream.getSplitSize(12 * 1024 * 1024));
      Assert.assertEquals(4, stream.getSplitSize(102 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(102));
      stream.close();

      stream = fs.new BatchByteArrayInputStream(url, new Path("/test1/test2"),
          4 * 1024 * 1024, 4);
      Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(8 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(16 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(12 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(102 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(102));
      stream.close();

      stream = fs.new BatchByteArrayInputStream(url, new Path("/test1/test2"),
          16 * 1024 * 1024, 2);
      Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(16 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(12 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(102 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(102));
      stream.close();

      stream = fs.new BatchByteArrayInputStream(url, new Path("/test1/test2"),
          8 * 1024 * 1024, 2);
      Assert.assertEquals(1, stream.getSplitSize(1 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(2 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(3 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(4 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(5 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(6 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(7 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(8 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(16 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(12 * 1024 * 1024));
      Assert.assertEquals(2, stream.getSplitSize(102 * 1024 * 1024));
      Assert.assertEquals(1, stream.getSplitSize(102));
      stream.close();

    } finally {
      mockWebHDFSServerClient.clear(getFileStatusRequest);
      mockOAuthServerClient.clear(oauthServerRequest);
    }
  }

}
