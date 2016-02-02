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

import com.eclipsesource.json.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.ConfCredentialBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2ConnectionConfigurator;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestBaseSetup {
    public static final Log LOG = LogFactory.getLog(TestAdlFileSystem.class);

    protected ClientAndServer mockWebHDFS;
    protected ClientAndServer mockOAuthServer;

    protected final static int WEBHDFS_PORT = 7552;
    protected final static int OAUTH_PORT = 7553;

    protected final static Header CONTENT_TYPE_APPLICATION_JSON = new Header("Content-Type", "application/json");

    protected final static String AUTH_TOKEN = "0123456789abcdef";
    protected final static Header AUTH_TOKEN_HEADER = new Header("AUTHORIZATION", OAuth2ConnectionConfigurator.HEADER + AUTH_TOKEN);

    static {
        LOG.info("Auth token header: " + AUTH_TOKEN_HEADER);
    }

    @Before
    public void startMockOAuthServer() {
        mockOAuthServer = startClientAndServer(OAUTH_PORT);
    }
    @Before
    public void startMockWebHDFSServer() {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));

        mockWebHDFS = startClientAndServer(WEBHDFS_PORT);
    }

    protected HttpRequest getOAuthServerMockRequest(MockServerClient mockServerClient) {
        HttpRequest expectedRequest = request()
                .withMethod("POST")
                .withPath("/refresh")
                        // TODO: rewrite with JsonBody now that no longer relying on unirest...
                .withBody("client_secret=credential&grant_type=client_credentials&client_id=MY_CLIENTID");

        JsonObject jsonObject = new JsonObject()
                .set(OAuth2Constants.EXPIRES_IN, "0987654321")
                .set("token_type", "bearer")
                .set(OAuth2Constants.ACCESS_TOKEN, AUTH_TOKEN);

        HttpResponse resp = response()
                .withStatusCode(HttpStatus.SC_OK)
                .withHeaders(
                        CONTENT_TYPE_APPLICATION_JSON
                )
                .withBody(jsonObject.toString());

        mockServerClient
                .when(expectedRequest, exactly(1))
                .respond(resp);

        return expectedRequest;
    }

    public Configuration getCaboConfiguration() {
        Configuration conf = new Configuration();

        // Configs for OAuth2
        //conf.set(OAuthConnectionConfigurator.OAUTH_ACCESS_TOKEN_KEY, AUTH_TOKEN);
        conf.set(HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY, "MY_CLIENTID");

        conf.set(HdfsClientConfigKeys.ACCESS_TOKEN_PROVIDER_KEY, ConfCredentialBasedAccessTokenProvider.class.getName());
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,"true");

        // Turn off SSL for test.
        // TODO: See how to handle SSL in MockServer, test with both on and off
        //conf.set(CaboWebHdfsFileSystem.CaboWebHDFSConfigs.TRANSPORT.config, "http");
        return conf;

    }

    @After
    public void stopMockWebHDFSServer() {
        mockWebHDFS.stop();
    }

    @After
    public void stopMockOAuthServer() {
        mockOAuthServer.stop();
    }
}
