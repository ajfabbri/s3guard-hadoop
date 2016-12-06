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

import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 *  Tests a live S3 system. If your keys and bucket aren't specified, all tests
 *  are marked as passed.
 *
 *  This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 *  TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 *  properly making it impossible to skip the tests if we don't have a valid
 *  bucket.
 **/
public class ITestS3AFileSystemContract extends FileSystemContractBaseTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileSystemContract.class);

  private Path basePath;

  @Rule
  public TestName methodName = new TestName();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    S3AFileSystem s3afs = S3ATestUtils.createTestFileSystem(conf);
    fs = s3afs;

    basePath = fs.makeQualified(
        S3ATestUtils.createTestPath(new Path("/s3afilesystemcontract")));
    MetadataStore ms = s3afs.getMetadataStore();
    // XXX nuke metadatastore contents
    ms.deleteSubtree(new Path(s3afs.getUri().toString(), "/"));
    super.setUp();
  }

  /**
   * This path explicitly places all absolute paths under the per-test suite
   * path directory; this allows the test to run in parallel.
   * @param pathString path string as input
   * @return a qualified path string.
   */
  protected Path path(String pathString) {
    if (pathString.startsWith("/")) {
      return fs.makeQualified(new Path(basePath, pathString));
    } else {
      return super.path(pathString);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(basePath, true);
    }
    super.tearDown();
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // not supported
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) {
      return;
    }

    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertFalse("Nested file1 exists",
                fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
                fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
               fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
               fs.exists(path("/test/new/newdir/subdir/file2")));
  }

//  @Override
  public void testMoveDirUnderParent() throws Throwable {
    // not support because
    // Fails if dst is a directory that is not empty.
  }

  @Override
  protected void writeAndRead(Path path, byte[] src, int len, boolean overwrite,
      boolean delete) throws IOException {

    LOG.debug("{} len {} overwrite {} delete {}", path, len, overwrite, delete);
    super.writeAndRead(path, src, len, overwrite, delete);
    LOG.debug("END {}", path);
  }

  @Override
  public void testWriteReadAndDeleteOneBlock() throws Exception {
    LOG.debug("ENTER");
    super.testWriteReadAndDeleteOneBlock();
    LOG.debug("EXIT");
  }
}
