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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.common.TestBaseSetup;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.Parameter;

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
public class TestDownloadBenchmark extends TestBaseSetup {

  @Test
  public void download4MBDataTest() throws URISyntaxException, IOException {
    MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost",
        WEBHDFS_PORT);
    MockServerClient mockOAuthServerClient = new MockServerClient("localhost",
        OAUTH_PORT);

    HttpRequest oauthServerRequest = getOAuthServerMockRequest(
        mockOAuthServerClient);
    byte[] data = TestADLResponseData.getRandomByteArrayData(8 * 1024 * 1024);

    HttpRequest getFileStatusRequest = request().withMethod("GET")
        .withPath("/webhdfs/v1/test1/test2")
        .withQueryStringParameter(new Parameter("op", "GETFILESTATUS"))
        .withHeader(AUTH_TOKEN_HEADER);

    HttpRequest fileSystemRequest = request().withMethod("GET")
        .withPath("/webhdfs/v1/test1/test2")
        .withQueryStringParameter(new Parameter("op", "OPEN"))
        .withHeader(AUTH_TOKEN_HEADER);

    try {
      mockWebHDFSServerClient.when(getFileStatusRequest, exactly(1)).respond(
          response().withStatusCode(HttpStatus.SC_OK).withBody(
              TestADLResponseData
                  .getGetFileStatusJSONResponse(64 * 1024 * 1024)));

      mockWebHDFSServerClient.when(fileSystemRequest, unlimited())
          .respond(response().withStatusCode(HttpStatus.SC_OK).withBody(data));

      FileSystem fs = new TestableAdlFileSystem();
      Configuration conf = getADLConfiguration();
      conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" +
          OAUTH_PORT + "/refresh");
      conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
          "credential");

      URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
      fs.initialize(uri, conf);

      FSDataInputStream in = fs.open(new Path("/test1/test2"));
      byte[] expectedData = new byte[4 * 1024 * 1024];
      for (int i = 0; i < 16; ++i) {
        long startTime = System.currentTimeMillis();
        in.read(expectedData);
        long endTime = System.currentTimeMillis();
        System.out.println(i + " Time : " + (endTime - startTime));
      }
      in.close();
      fs.close();
    } finally {
      mockWebHDFSServerClient.clear(fileSystemRequest);
      mockWebHDFSServerClient.clear(getFileStatusRequest);
      mockOAuthServerClient.clear(oauthServerRequest);
    }
  }

  @Test
  public void download4MBDataMultipleFileSystemInstanceTest()
      throws URISyntaxException, IOException {
    MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost",
        WEBHDFS_PORT);
    MockServerClient mockOAuthServerClient = new MockServerClient("localhost",
        OAUTH_PORT);

    HttpRequest oauthServerRequest = getOAuthServerMockRequest(
        mockOAuthServerClient);
    byte[] data = TestADLResponseData.getRandomByteArrayData(8 * 1024 * 1024);

    HttpRequest getFileStatusRequest = request().withMethod("GET")
        .withPath("/webhdfs/v1/test1/test2")
        .withQueryStringParameter(new Parameter("op", "GETFILESTATUS"))
        .withHeader(AUTH_TOKEN_HEADER);

    HttpRequest fileSystemRequest = request().withMethod("GET")
        .withPath("/webhdfs/v1/test1/test2")
        .withQueryStringParameter(new Parameter("op", "OPEN"))
        .withHeader(AUTH_TOKEN_HEADER);

    try {
      mockWebHDFSServerClient.when(getFileStatusRequest, exactly(1)).respond(
          response().withStatusCode(HttpStatus.SC_OK).withBody(
              TestADLResponseData
                  .getGetFileStatusJSONResponse(64 * 1024 * 1024)));

      mockWebHDFSServerClient.when(fileSystemRequest, unlimited())
          .respond(response().withStatusCode(HttpStatus.SC_OK).withBody(data));

      FileSystem fs = new TestableAdlFileSystem();
      FileSystem anotherFs = new TestableAdlFileSystem();
      Configuration conf = getADLConfiguration();
      conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" +
          OAUTH_PORT + "/refresh");
      conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
          "credential");

      URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
      fs.initialize(uri, conf);
      anotherFs.initialize(uri, conf);

      FSDataInputStream in = fs.open(new Path("/test1/test2"));
      FSDataInputStream anotherIn = anotherFs.open(new Path("/test1/test2"));
      byte[] expectedData = new byte[4 * 1024 * 1024];
      for (int i = 0; i < 16; ++i) {
        long startTime = System.currentTimeMillis();
        in.read(expectedData);
        long endTime = System.currentTimeMillis();
        System.out.println(i + " Time : " + (endTime - startTime));
        startTime = System.currentTimeMillis();
        anotherIn.read(expectedData);
        endTime = System.currentTimeMillis();
        System.out.println(i + " Time : " + (endTime - startTime));
      }

      in.close();
      anotherIn.close();
      anotherFs.close();
      fs.close();
    } finally {
      mockWebHDFSServerClient.clear(fileSystemRequest);
      mockWebHDFSServerClient.clear(getFileStatusRequest);
      mockOAuthServerClient.clear(oauthServerRequest);
    }
  }
}
