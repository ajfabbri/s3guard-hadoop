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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Main test class for MetadataStore implementations.
 * Implementations should each create a test by subclassing this and
 * overriding {@link #createContract()}.
 * If your implementation may return missing results for recently set paths,
 * override {@link MetadataStoreTestBase#allowMissing()}.
 */
public abstract class MetadataStoreTestBase extends Assert {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetadataStoreTestBase.class);

  /** Some dummy values for sanity-checking FileStatus contents. */
  static final long BLOCK_SIZE = 32 * 1024 * 1024;
  static final int REPLICATION = 1;
  static final FsPermission PERMISSION = new FsPermission((short)0755);
  static final String OWNER = "bob";
  static final String GROUP = "uncles";
  private final long accessTime = System.currentTimeMillis();
  private final long modTime = accessTime - 5000;

  /**
   * Each test should override this.
   * @return Contract which specifies the MetadataStore under test plus config.
   */
  public abstract AbstractMSContract createContract() throws IOException;

  /**
   * Tests assume that implementations will return recently set results.  If
   * your implementation does not always hold onto metadata (e.g. LRU or
   * time-based expiry) you can override this to return false.
   * @return true if the test should succeed when null results are returned
   *  from the MetadataStore under test.
   */
  public boolean allowMissing() {
    return false;
  }

  /** The MetadataStore contract used to test against. */
  private AbstractMSContract contract;

  private MetadataStore ms;

  @Before
  public void setUp() throws Exception {
    LOG.debug("== Setup. ==");
    contract = createContract();
    ms = contract.getMetadataStore();
    assertNotNull("null MetadataStore", ms);
    assertNotNull("null FileSystem", contract.getFileSystem());
    ms.initialize(contract.getFileSystem());
  }

  @After
  public void tearDown() throws Exception {
    LOG.debug("== Tear down. ==");
    if (ms != null) {
      ms.destroy();
      ms = null;
    }
  }

  /**
   * Test that we can get the whole sub-tree by iterating DescendantsIterator.
   *
   * The tree is similar to or same as the example in code comment.
   */
  @Test
  public void testDescendantsIterator() throws IOException {
    final String[] tree = new String[] {
        "/dir1",
        "/dir1/dir2",
        "/dir1/dir3",
        "/dir1/dir2/file1",
        "/dir1/dir2/file2",
        "/dir1/dir3/dir4",
        "/dir1/dir3/dir5",
        "/dir1/dir3/dir4/file3",
        "/dir1/dir3/dir5/file4",
        "/dir1/dir3/dir6"
    };
    // we set up the example file system tree in metadata store
    for (String pathStr : tree) {
      final FileStatus status = pathStr.contains("file")
          ? basicFileStatus(strToPath(pathStr), 100, false)
          : basicFileStatus(strToPath(pathStr), 0, true);
      ms.put(new PathMetadata(status));
    }

    final Set<String> actual = new HashSet<>();
    final PathMetadata rootMeta = new PathMetadata(makeDirStatus("/"));
    for (DescendantsIterator desc = new DescendantsIterator(ms, rootMeta);
         desc.hasNext();) {
      final Path p = desc.next().getFileStatus().getPath();
      actual.add(Path.getPathWithoutSchemeAndAuthority(p).toString());
    }
    LOG.info("We got {} by iterating DescendantsIterator", actual);

    if (!allowMissing()) {
      assertEquals(Sets.newHashSet(tree), actual);
    }
  }

  @Test
  public void testPutNew() throws Exception {
    /* create three dirs /da1, /da2, /da3 */
    createNewDirs("/da1", "/da2", "/da3");

    /* It is caller's responsibility to set up ancestor entries beyond the
     * containing directory.  We only track direct children of the directory.
     * Thus this will not affect entry for /da1.
     */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)));

    assertEmptyDirs("/da2", "/da3");
    assertDirectorySize("/da1/db1", 1);

    /* Check contents of dir status. */
    PathMetadata dirMeta = ms.get(strToPath("/da1"));
    if (!allowMissing() || dirMeta != null) {
      verifyDirStatus(dirMeta.getFileStatus());
    }

    /* This already exists, and should silently replace it. */
    ms.put(new PathMetadata(makeDirStatus("/da1/db1")));

    /* If we had putNew(), and used it above, this would be empty again. */
    assertDirectorySize("/da1", 1);

    assertEmptyDirs("/da2", "/da3");

    /* Ensure new files update correct parent dirs. */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)));
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc2", 200)));
    assertDirectorySize("/da1", 1);
    assertDirectorySize("/da1/db1", 2);
    assertEmptyDirs("/da2", "/da3");
    PathMetadata meta = ms.get(strToPath("/da1/db1/fc2"));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get file after put new.", meta);
      verifyFileStatus(meta.getFileStatus(), 200);
    }
  }

  @Test
  public void testPutOverwrite() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)));
    ms.put(new PathMetadata(makeDirStatus(dirPath)));
    PathMetadata meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    ms.put(new PathMetadata(basicFileStatus(strToPath(filePath), 9999, false)));
    meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      verifyFileStatus(meta.getFileStatus(), 9999);
    }
  }

  @Test
  public void testRootDirPutNew() throws Exception {
    Path rootPath = strToPath("/");

    ms.put(new PathMetadata(makeFileStatus("/file1", 100)));
    DirListingMetadata dir = ms.listChildren(rootPath);
    if (!allowMissing() || dir != null) {
      assertNotNull("Root dir cached", dir);
      assertFalse("Root not fully cached", dir.isAuthoritative());
      assertNotNull("have root dir file listing", dir.getListing());
      assertEquals("One file in root dir", 1, dir.getListing().size());
      assertEquals("file1 in root dir", strToPath("/file1"),
          dir.getListing().iterator().next().getFileStatus().getPath());
    }
  }

  @Test
  public void testDelete() throws Exception {
    setUpDeleteTest();

    ms.delete(strToPath("/ADirectory1/db1/file2"));

    /* Ensure delete happened. */
    assertDirectorySize("/ADirectory1/db1", 1);
    PathMetadata meta = ms.get(strToPath("/ADirectory1/db1/file2"));
    assertNull("File deleted", meta);
  }

  @Test
  public void testDeleteSubtree() throws Exception {
    deleteSubtreeHelper("");
  }

  @Test
  public void testDeleteSubtreeHostPath() throws Exception {
    deleteSubtreeHelper(contract.getFileSystem().getUri().toString());
  }

  private void deleteSubtreeHelper(String pathPrefix) throws Exception {

    String p = pathPrefix;
    setUpDeleteTest(p);
    createNewDirs(p + "/ADirectory1/db1/dc1", p + "/ADirectory1/db1/dc1/dd1");
    ms.put(new PathMetadata(
        makeFileStatus(p + "/ADirectory1/db1/dc1/dd1/deepFile", 100)));
    if (!allowMissing()) {
      assertCached(p + "/ADirectory1/db1");
    }
    ms.deleteSubtree(strToPath(p + "/ADirectory1/db1/"));

    assertEmptyDirectory(p + "/ADirectory1");
    assertNotCached(p + "/ADirectory1/db1");
    assertNotCached(p + "/ADirectory1/file1");
    assertNotCached(p + "/ADirectory1/file2");
    assertNotCached(p + "/ADirectory1/db1/dc1/dd1/deepFile");
    assertEmptyDirectory(p + "/ADirectory2");
  }


  /*
   * Some implementations might not support this.  It was useful to test
   * correctness of the LocalMetadataStore implementation, but feel free to
   * override this to be a no-op.
   */
  @Test
  public void testDeleteRecursiveRoot() throws Exception {
    setUpDeleteTest();

    ms.deleteSubtree(strToPath("/"));
    assertNotCached("/ADirectory1");
    assertNotCached("/ADirectory2");
    assertNotCached("/ADirectory2/db1");
    assertNotCached("/ADirectory2/db1/file1");
    assertNotCached("/ADirectory2/db1/file2");
  }

  @Test
  public void testDeleteNonExisting() throws Exception {
    // Path doesn't exist, but should silently succeed
    ms.delete(strToPath("/bobs/your/uncle"));

    // Ditto.
    ms.deleteSubtree(strToPath("/internets"));
  }


  private void setUpDeleteTest() throws IOException {
    setUpDeleteTest("");
  }

  private void setUpDeleteTest(String prefix) throws IOException {
    createNewDirs(prefix + "/ADirectory1", prefix + "/ADirectory2",
        prefix + "/ADirectory1/db1");
    ms.put(new PathMetadata(makeFileStatus(prefix + "/ADirectory1/db1/file1",
        100)));
    ms.put(new PathMetadata(makeFileStatus(prefix + "/ADirectory1/db1/file2",
        100)));

    PathMetadata meta = ms.get(strToPath(prefix + "/ADirectory1/db1/file2"));
    if (!allowMissing() || meta != null) {
      assertNotNull("Found test file", meta);
      assertDirectorySize(prefix + "/ADirectory1/db1", 2);
    }
  }

  @Test
  public void testGet() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)));
    ms.put(new PathMetadata(makeDirStatus(dirPath)));
    PathMetadata meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file", meta);
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    meta = ms.get(strToPath(dirPath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file (dir)", meta);
      assertTrue("Found dir", meta.getFileStatus().isDirectory());
    }

    meta = ms.get(strToPath("/bollocks"));
    assertNull("Don't get non-existent file", meta);
  }


  @Test
  public void testListChildren() throws Exception {
    setupListStatus();

    DirListingMetadata dirMeta;
    dirMeta = ms.listChildren(strToPath("/"));
    if (!allowMissing()) {
      assertNotNull(dirMeta);
        /* Cache has no way of knowing it has all entries for root unless we
         * specifically tell it via put() with
         * DirListingMetadata.isAuthoritative = true */
      assertFalse("Root dir is not cached, or partially cached",
          dirMeta.isAuthoritative());
      assertListingsEqual(dirMeta.getListing(), "/a1", "/a2");
    }

    dirMeta = ms.listChildren(strToPath("/a1"));
    if (!allowMissing() || dirMeta != null) {
      assertListingsEqual(dirMeta.getListing(), "/a1/b1", "/a1/b2");
    }

    // TODO
    // 1. Add properties query to MetadataStore interface
    // supportsAuthoritativeDirectories() or something.
    // 2. Add "isNew" flag to MetadataStore.put(DirListingMetadata)
    // 3. If #1 is true, assert that directory is still fully cached here.
    // assertTrue("Created dir is fully cached", dirMeta.isAuthoritative());

    dirMeta = ms.listChildren(strToPath("/a1/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertListingsEqual(dirMeta.getListing(), "/a1/b1/file1", "/a1/b1/file2",
          "/a1/b1/c1");
    }
  }

  @Test
  public void testDirListingRoot() throws Exception {
    commonTestPutListStatus("/");
  }

  @Test
  public void testPutDirListing() throws Exception {
    commonTestPutListStatus("/a");
  }

  @Test
  public void testInvalidListChildren() throws Exception {
    setupListStatus();
    assertNull("missing path returns null",
        ms.listChildren(strToPath("/a1/b1x")));
  }

  @Test
  public void testMove() throws Exception {
    // Create test dir structure
    createNewDirs("/a1", "/a2", "/a3");
    createNewDirs("/a1/b1", "/a1/b2");
    putListStatusFiles("/a1/b1", false, "/a1/b1/file1", "/a1/b1/file2");

    // Assert root listing as expected
    Collection<PathMetadata> entries;
    DirListingMetadata dirMeta = ms.listChildren(strToPath("/"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("Listing root", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/a1", "/a2", "/a3");
    }

    // Assert src listing as expected
    dirMeta = ms.listChildren(strToPath("/a1/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("Listing /a1/b1", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/a1/b1/file1", "/a1/b1/file2");
    }

    // Do the move(): rename(/a1/b1, /b1)
    Collection<Path> srcPaths = Arrays.asList(strToPath("/a1/b1"),
        strToPath("/a1/b1/file1"), strToPath("/a1/b1/file2"));

    ArrayList<PathMetadata> destMetas = new ArrayList<>();
    destMetas.add(new PathMetadata(makeDirStatus("/b1")));
    destMetas.add(new PathMetadata(makeFileStatus("/b1/file1", 100)));
    destMetas.add(new PathMetadata(makeFileStatus("/b1/file2", 100)));
    ms.move(srcPaths, destMetas);

    // Assert src is no longer there
    dirMeta = ms.listChildren(strToPath("/a1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("Listing /a1", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/a1/b2");
    }

    PathMetadata meta = ms.get(strToPath("/a1/b1/file1"));
    // TODO allow return of PathMetadata with isDeleted == true
    assertNull("Src path deleted", meta);

    // Assert dest looks right
    meta = ms.get(strToPath("/b1/file1"));
    if (!allowMissing() || meta != null) {
      assertNotNull("dest file not null", meta);
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    dirMeta = ms.listChildren(strToPath("/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("dest listing not null", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/b1/file1", "/b1/file2");
    }
  }


  /*
   * Helper functions.
   */

  /** Builds array of Path objects based on parent dir and filenames. */
  private Path[] buildPaths(String parent, String... paths) {

    Path[] output = new Path[paths.length];
    for (int i = 0; i < paths.length; i++) {
      output[i] = new Path(strToPath(parent), paths[i]);
    }
    return output;
  }

  /** Modifies paths input array and returns it. */
  private String[] buildPathStrings(String parent, String... paths) {
    for (int i = 0; i < paths.length; i++) {
      Path p = new Path(strToPath(parent), paths[i]);
      paths[i] = p.toString();
    }
    return paths;
  }

  private void commonTestPutListStatus(final String parent) throws IOException {
    putListStatusFiles(parent, true, buildPathStrings(parent, "file1", "file2",
        "file3"));
    DirListingMetadata dirMeta = ms.listChildren(strToPath(parent));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("list after putListStatus", dirMeta);
      Collection<PathMetadata> entries = dirMeta.getListing();
      assertNotNull("listStatus has entries", entries);
      assertListingsEqual(entries,
          buildPathStrings(parent, "file1", "file2", "file3"));
    }
  }

  private void setupListStatus() throws IOException {
    createNewDirs("/a1", "/a2", "/a1/b1", "/a1/b2", "/a1/b1/c1",
        "/a1/b1/c1/d1");
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file1", 100)));
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file2", 100)));
  }

  private void assertListingsEqual(Collection<PathMetadata> listing,
      String ...pathStrs) {
    Set<Path> a = new HashSet<>();
    for (PathMetadata meta : listing) {
      a.add(meta.getFileStatus().getPath());
    }

    Set<Path> b = new HashSet<>();
    for (String ps : pathStrs) {
      b.add(strToPath(ps));
    }
    assertEquals("Same set of files", b, a);
  }

  private void putListStatusFiles(String dirPath, boolean authoritative,
      String... filenames) throws IOException {
    ArrayList<PathMetadata> metas = new ArrayList<>(filenames .length);
    for (int i = 0; i < filenames.length; i++) {
      metas.add(new PathMetadata(makeFileStatus(filenames[i], 100)));
    }
    DirListingMetadata dirMeta =
        new DirListingMetadata(strToPath(dirPath), metas, authoritative);
    ms.put(dirMeta);
  }

  private void createNewDirs(String... dirs)
      throws IOException {
    for (String pathStr : dirs) {
      ms.put(new PathMetadata(makeDirStatus(pathStr)));
    }
  }

  private void assertDirectorySize(String pathStr, int size)
      throws IOException {
    DirListingMetadata dirMeta = ms.listChildren(strToPath(pathStr));
    if (!allowMissing()) {
      assertNotNull("Directory " + pathStr + " in cache", dirMeta);
    }
    if (!allowMissing() || dirMeta != null) {
      assertEquals("Number of entries in dir " + pathStr, size,
          nonDeleted(dirMeta.getListing()).size());
    }
  }

  /** @return only file statuses which are *not* marked deleted. */
  private Collection<PathMetadata> nonDeleted(
      Collection<PathMetadata> statuses) {
    /* TODO: filter out paths marked for deletion. */
    return statuses;
  }

  private void assertNotCached(String pathStr) throws IOException {
    // TODO this should return an entry with deleted flag set
    Path path = strToPath(pathStr);
    PathMetadata meta = ms.get(path);
    assertNull(pathStr + " should not be cached.", meta);
  }

  private void assertCached(String pathStr) throws IOException {
    Path path = strToPath(pathStr);
    PathMetadata meta = ms.get(path);
    assertNotNull(pathStr + " should be cached.", meta);
  }

  /**
   * Convenience to create a fully qualified Path from string.
   */
  private Path strToPath(String p) {
    final Path path = new Path(p);
    assert path.isAbsolute();
    return path.makeQualified(contract.getFileSystem().getUri(), null);
  }

  private void assertEmptyDirectory(String pathStr) throws IOException {
    assertDirectorySize(pathStr, 0);
  }

  private void assertEmptyDirs(String ...dirs) throws IOException {
    for (String pathStr : dirs) {
      assertEmptyDirectory(pathStr);
    }
  }

  FileStatus basicFileStatus(Path path, int size, boolean isDir)
      throws IOException {
    return new FileStatus(size, isDir, REPLICATION, BLOCK_SIZE, modTime,
        accessTime, PERMISSION, OWNER, GROUP, path);
  }

  private FileStatus makeFileStatus(String pathStr, int size)
      throws IOException {
    return basicFileStatus(strToPath(pathStr), size, false);
  }

  void verifyFileStatus(FileStatus status, long size) {
    assertFalse("Not a dir", status.isDirectory());
    assertEquals("Mod time", modTime, status.getModificationTime());
    assertEquals("File size", size, status.getLen());
    assertEquals("Block size", BLOCK_SIZE, status.getBlockSize());
  }

  private FileStatus makeDirStatus(String pathStr) throws IOException {
    return basicFileStatus(strToPath(pathStr), 0, true);
  }

  /**
   * Verify the directory file status. Subclass may verify additional fields.
   */
  void verifyDirStatus(FileStatus status) {
    assertTrue("Is a dir", status.isDirectory());
    assertEquals("zero length", 0, status.getLen());
  }

  long getModTime() {
    return modTime;
  }

  long getAccessTime() {
    return accessTime;
  }

}