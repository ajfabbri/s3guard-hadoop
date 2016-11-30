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
 */

package org.apache.hadoop.fs.s3a.s3guard;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Logic for creating DynamoDB clients.  For now there is only one of these,
 * but we could generalize al la {@link S3ClientFactory} in the future if we
 * want create a separate mock implementation.
 */
public class DynamoDBClientFactory extends Configured {

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamoDBClientFactory.class);

  public DynamoDBClientFactory(Configuration conf) {
    super(conf);
  }

  /**
   * Create a new {@link AmazonDynamoDBClient} client.
   *
   * @param fsUri validated form of S3A file system URI DynamoDB client relates
   * @param region the region of the client connects to
   * @return a DynamoDB client in the provided region
   * @throws IOException when IO problem met
   */
  public AmazonDynamoDBClient create(URI fsUri, Region region)
      throws IOException {
    final Configuration conf = getConf();
    final AWSCredentialsProvider credentials =
        S3AUtils.createAWSCredentialProviderSet(fsUri, conf, fsUri);
    final ClientConfiguration awsConf = DefaultS3ClientFactory.createAWSConf(
        conf);
    AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials, awsConf);

    Preconditions.checkNotNull(region);
    ddb.withRegion(region);
    final String endPoint = conf.get(Constants.S3GUARD_DDB_ENDPOINT_KEY);
    if (StringUtils.isNotEmpty(endPoint)) {
      try {
        ddb.withEndpoint(conf.get(Constants.S3GUARD_DDB_ENDPOINT_KEY));
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect DynamoDB endpoint: "  + endPoint;
        LOG.error(msg, e);
        throw new IOException(msg, e);
      }
    }
    return ddb;
  }
}
