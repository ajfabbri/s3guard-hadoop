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

package com.microsoft.azure.datalake.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;


public class TestListStatus extends TestBaseSetup {

    @Test
    public void listStatusReturnsAsExpected() throws URISyntaxException, IOException {
        MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost", WEBHDFS_PORT);
        MockServerClient mockOAuthServerClient = new MockServerClient("localhost", OAUTH_PORT);

        HttpRequest oauthServerRequest = getOAuthServerMockRequest(mockOAuthServerClient);

        HttpRequest fileSystemRequest = request()
                .withMethod("GET")
                .withPath("/webhdfs/v1/test1/test2")
                .withHeader(AUTH_TOKEN_HEADER);

        try {
            mockWebHDFSServerClient.when(fileSystemRequest,
                    exactly(1)
            )
                    .respond(
                            response()
                                    .withStatusCode(HttpStatus.SC_OK)
                                    .withHeaders(
                                            CONTENT_TYPE_APPLICATION_JSON
                                    )
                                    .withBody(TestADLResponseData.getListFileStatusJSONResponse(2048))
                    );

            FileSystem fs = new TestableAdlFileSystem();
            Configuration conf = getCaboConfiguration();
            conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" + OAUTH_PORT + "/refresh");
            conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY, "credential");

            URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
            fs.initialize(uri, conf);

            long startTime = System.currentTimeMillis();
            FileStatus[] ls = fs.listStatus(new Path("/test1/test2"));
            long endTime = System.currentTimeMillis();

            System.out.println("Time : " + (endTime-startTime));

            mockOAuthServer.verify(oauthServerRequest);
            mockWebHDFSServerClient.verify(fileSystemRequest);

            fs.close();
        } finally {
            mockWebHDFSServerClient.clear(fileSystemRequest);
            mockOAuthServerClient.clear(oauthServerRequest);
        }
    }
}
