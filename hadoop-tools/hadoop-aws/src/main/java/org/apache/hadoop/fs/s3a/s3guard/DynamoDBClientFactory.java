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

import java.io.IOException;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;

/**
 * Interface to create a DynamoDB client.
 */
interface DynamoDBClientFactory {
  Logger LOG = LoggerFactory.getLogger(DynamoDBClientFactory.class);

  AmazonDynamoDBClient createDynamoDBClient(URI fsUri, Region region)
      throws IOException;

  /**
   * The default implementation for creating an AmazonDynamoDBClient.
   *
   * This implementation should extend {@link Configured} class for setting and
   * getting configuration.
   */
  class DefaultDynamoDBClientFactory extends Configured
      implements DynamoDBClientFactory {
    @Override
    public AmazonDynamoDBClient createDynamoDBClient(URI fsUri, Region region)
        throws IOException {
      Preconditions.checkNotNull(region,
          "Null region found; should use the region as S3 bucket");

      final Configuration conf = getConf();
      final AWSCredentialsProvider credentials =
          createAWSCredentialProviderSet(fsUri, conf, fsUri);
      final ClientConfiguration awsConf =
          DefaultS3ClientFactory.createAwsConf(conf);
      AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials, awsConf);

      ddb.withRegion(region);
      final String endPoint = conf.get(S3Guard.S3GUARD_DDB_ENDPOINT_KEY);
      if (StringUtils.isNotEmpty(endPoint)) {
        try {
          ddb.withEndpoint(conf.get(S3Guard.S3GUARD_DDB_ENDPOINT_KEY));
        } catch (IllegalArgumentException e) {
          String msg = "Incorrect DynamoDB endpoint: "  + endPoint;
          LOG.error(msg, e);
          throw new IllegalArgumentException(msg, e);
        }
      }
      return ddb;
    }
  }

}
