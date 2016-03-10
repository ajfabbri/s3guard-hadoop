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

import org.apache.hadoop.fs.adl.TestableAdlFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.common.TestBaseSetup;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestConfigurationSetting extends TestBaseSetup {

  @Test
  public void testAllConfiguration() throws URISyntaxException, IOException {
    TestableAdlFileSystem fs = new TestableAdlFileSystem();
    Configuration conf = getADLConfiguration();
    conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY, "http://localhost:" +
        OAUTH_PORT + "/refresh");
    conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
        "credential");

    URI uri = new URI("adl://localhost:" + WEBHDFS_PORT);
    fs.initialize(uri, conf);

    // Default setting check
    Assert.assertEquals(true, fs.isFeatureRedirectOff());
    Assert.assertEquals(true, fs.isFeatureCacheFileStatus());
    Assert.assertEquals(5, fs.getFeatureCacheFileStatusDuration());
    Assert.assertEquals(true, fs.isFeatureGetBlockLocationLocallyBundled());
    Assert.assertEquals(true, fs.isFeatureConcurrentReadWithReadAhead());
    Assert.assertEquals(false, fs.isOverrideOwnerFeatureOn());
    Assert.assertEquals(16 * 1024 * 1024, fs.getMaxBufferSize());
    Assert.assertEquals(2, fs.getMaxConcurrentConnection());

    fs.close();

    // Configuration toggle check
    conf.set("adl.feature.override.redirection.off", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureRedirectOff());
    fs.close();
    conf.set("adl.feature.override.redirection.off", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureRedirectOff());
    fs.close();

    conf.set("adl.feature.override.cache.filestatus", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureCacheFileStatus());
    fs.close();
    conf.set("adl.feature.override.cache.filestatus", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureCacheFileStatus());
    fs.close();

    conf.set("adl.feature.override.cache.filestatus.duration", "1");
    fs.initialize(uri, conf);
    Assert.assertEquals(1, fs.getFeatureCacheFileStatusDuration());
    fs.close();
    conf.set("adl.feature.override.cache.filestatus.duration", "1000");
    fs.initialize(uri, conf);
    Assert.assertEquals(1000, fs.getFeatureCacheFileStatusDuration());
    fs.close();

    conf.set("adl.feature.override.getblocklocation.locally.bundled",
        "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureGetBlockLocationLocallyBundled());
    fs.close();
    conf.set("adl.feature.override.getblocklocation.locally.bundled",
        "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureGetBlockLocationLocallyBundled());
    fs.close();

    conf.set("adl.feature.override.readahead", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureConcurrentReadWithReadAhead());
    fs.close();
    conf.set("adl.feature.override.readahead", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureConcurrentReadWithReadAhead());
    fs.close();

    conf.set(
        "adl.feature.override.readahead.max.buffersize",
        "101");
    fs.initialize(uri, conf);
    Assert.assertEquals(101, fs.getMaxBufferSize());
    fs.close();
    conf.set(
        "adl.feature.override.readahead.max.buffersize",
        "12134565");
    fs.initialize(uri, conf);
    Assert.assertEquals(12134565, fs.getMaxBufferSize());
    fs.close();

    conf.set("adl.debug.override.localuserasfileowner", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isOverrideOwnerFeatureOn());
    fs.close();
    conf.set("adl.debug.override.localuserasfileowner", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isOverrideOwnerFeatureOn());
    fs.close();
  }
}
