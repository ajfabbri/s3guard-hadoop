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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Dest logic around whether or not a directory is empty, with S3Guard enabled.
 * The fact that S3AFileStatus has an isEmptyDirectory flag in it makes caching
 * S3AFileStatus's really tricky, as the flag can change as a side effect of
 * changes to other paths.
 * After S3Guard is merged to trunk, we should try to remove the
 * isEmptyDirectory flag from S3AFileStatus, or maintain it outside
 * of the MetadataStore.
 */
public class ITestS3GuardEmptyDirs extends AbstractS3ATestBase {

  /**
   * This test only runs if there is a non-Null MetadataStore configured.
   * @param conf  Configuration
   * @return test contract
   */
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    AbstractFSContract contract =  super.createContract(conf);
    if (S3Guard.isNullMetadataStoreConfigured(contract.getConf())) {
      LOG.debug("Skipping this test since NullMetadataStore is configured.");
      contract.setEnabled(false);
    }
    return contract;
  }

  @Override
  protected Configuration getConfiguration() {
    Configuration conf = super.getConfiguration();
    // XXX HACK
    conf.setBoolean(Constants.S3GUARD_DDB_TABLE_CREATE_KEY, true);
    return conf;
  }

  @Test
  public void testEmptyDirs() throws Exception {

    S3AFileSystem fs = getFileSystem();
    MetadataStore configuredMs = fs.getMetadataStore();

    // 1. Simulate files already existing in the bucket before we started our
    // cluster.  Temporarily disable the MetadataStore so it doesn't witness
    // us creating these files.

    fs.setMetadataStore(new NullMetadataStore());

    Path existingDir = path("existing-dir");
    assertTrue(fs.mkdirs(existingDir));
    Path existingFile = path("existing-dir/existing-file");
    touch(fs, existingFile);


    // 2. Simulate (from MetadataStore's perspective) starting our cluster and
    // creating a file in an existing directory.
    fs.setMetadataStore(configuredMs);  // "start cluster"
    Path newFile = path("existing-dir/new-file");
    touch(fs, newFile);

    S3AFileStatus status = fs.getFileStatus(existingDir);
    assertFalse("Should not be empty dir", status.isEmptyDirectory());

    // 3. Assert that removing the only file the MetadataStore witnessed
    // being created doesn't cause it to think the directory is now empty.
    fs.delete(newFile, false);
    status = fs.getFileStatus(existingDir);
    assertFalse("Should not be empty dir", status.isEmptyDirectory());

    // 4. Assert that removing the final file, that existed "before"
    // MetadataStore started, *does* cause the directory to be marked empty.
    fs.delete(existingFile, false);
    status = fs.getFileStatus(existingDir);
    assertTrue("Should be empty dir now", status.isEmptyDirectory());
  }
}
