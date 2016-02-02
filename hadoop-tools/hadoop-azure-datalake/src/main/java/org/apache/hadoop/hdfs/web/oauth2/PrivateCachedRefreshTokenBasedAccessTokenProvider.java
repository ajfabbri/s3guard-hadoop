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

package org.apache.hadoop.hdfs.web.oauth2;

import java.io.IOException;

/**
 * Exposing AccessTokenProvider publicly to extend in com.microsoft.azure.datalake package. Extended version to cache
 * token for the process to gain performance gain.
 */
public abstract class PrivateCachedRefreshTokenBasedAccessTokenProvider extends AccessTokenProvider {
    /**
     * Implement to return OAuth2 access token (Bearer token)
     *
     * @return OAuth2 access token (Bearer token)
     * @throws IOException when system error, internal server error or user error
     */
    @Override
    public abstract String getAccessToken() throws IOException;
}
