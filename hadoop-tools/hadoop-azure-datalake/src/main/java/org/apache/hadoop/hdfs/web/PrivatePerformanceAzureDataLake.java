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
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Use this class implementation to log performance matrics as part of the debug logs. Used during development.
 */
public class PrivatePerformanceAzureDataLake extends DelegateToFileSystem {
    public static final int DEFAULT_PORT = 443;

    PrivatePerformanceAzureDataLake(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, createPrivateAzureDataLakeFileSystem(conf), conf,
                PrivatePerformanceAzureDataLakeFileSystem.SCHEME, false);
    }

    private static PrivatePerformanceAzureDataLakeFileSystem createPrivateAzureDataLakeFileSystem(
            Configuration conf) {
        PrivatePerformanceAzureDataLakeFileSystem fs = new PrivatePerformanceAzureDataLakeFileSystem();
        fs.setConf(conf);
        return fs;
    }

    @Override
    public final int getUriDefaultPort() {
        return DEFAULT_PORT;
    }
}
