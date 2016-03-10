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

package org.apache.hadoop.fs.adl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.TestBaseSetup;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.model.HttpRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.matchers.Times.unlimited;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Ensure TestAAAAAUploadBenchmark test gets executed before any other test.
 * Caused due to hang in test cases when
 * executed after other GET methods related test cases.
 */
public class TestUploadBenchmark extends TestBaseSetup {

  @Test
  public void upload4MBDataTest() throws URISyntaxException, IOException {
    MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost",
        WEBHDFS_PORT);
    MockServerClient mockOAuthServerClient = new MockServerClient("localhost",
        OAUTH_PORT);

    HttpRequest oauthServerRequest = getOAuthServerMockRequest(
        mockOAuthServerClient);

    HttpRequest fileSystemRequest = request().withMethod("PUT")
        .withPath("/webhdfs/v1/test1/test2").withHeader(AUTH_TOKEN_HEADER);

    HttpRequest fileSystemRequest1 = request().withMethod("POST")
        .withPath("/webhdfs/v1/test1/test2").withHeader(AUTH_TOKEN_HEADER);

    try {
      mockWebHDFSServerClient.when(fileSystemRequest, exactly(1))
          .respond(response().withStatusCode(HttpStatus.SC_CREATED));

      FileSystem fs = new TestableAdlFileSystem();
      Configuration conf = getADLConfiguration();
      conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" +
          OAUTH_PORT + "/refresh");
      conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
          "credential");

      URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
      fs.initialize(uri, conf);

      byte[] data = TestADLResponseData.getRandomByteArrayData();

      FSDataOutputStream out = fs.create(new Path("/test1/test2"));
      mockWebHDFSServerClient.when(fileSystemRequest1, unlimited())
          .respond(response().withStatusCode(HttpStatus.SC_OK));

      long startTime = System.currentTimeMillis();
      out.write(data);
      out.flush();
      long endTime = System.currentTimeMillis();
      System.out.println(" Time : " + (endTime - startTime));

      out.close();

      fs.close();
    } finally {
      mockWebHDFSServerClient.clear(fileSystemRequest);
      mockWebHDFSServerClient.clear(fileSystemRequest1);
      mockOAuthServerClient.clear(oauthServerRequest);
    }
  }
}
