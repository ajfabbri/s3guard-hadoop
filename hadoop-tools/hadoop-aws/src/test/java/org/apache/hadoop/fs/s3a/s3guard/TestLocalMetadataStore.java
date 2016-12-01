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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * MetadataStore unit test for {@link LocalMetadataStore}.
 */
public class TestLocalMetadataStore extends MetadataStoreTestBase {

  private static final String MAX_ENTRIES_STR = "16";

  private static class LocalMSContract extends AbstractMSContract {

    private FileSystem fs;

    public LocalMSContract() {
      Configuration config = new Configuration();
      config.set(LocalMetadataStore.CONF_MAX_RECORDS, MAX_ENTRIES_STR);
      try {
        fs = FileSystem.getLocal(config);
      } catch (IOException e) {
        fail("Error creating LocalFileSystem");
      }
    }

    @Override
    public FileSystem getFileSystem() {
      return fs;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      LocalMetadataStore lms = new LocalMetadataStore();
      return lms;
    }
  }

  @Override
  public AbstractMSContract createContract() {
    return new LocalMSContract();
  }

  @Test
  public void testClearByAncestor() {
    Map<Path, String> map = new HashMap<>();

    // 1. Test paths without scheme/host
    assertClearResult(map, "", "/", 0);
    assertClearResult(map, "", "/dirA/dirB", 2);
    assertClearResult(map, "", "/invalid", 5);


    // 2. Test paths w/ scheme/host
    String p = "s3a://fake-bucket-name";
    assertClearResult(map, p, "/", 0);
    assertClearResult(map, p, "/dirA/dirB", 2);
    assertClearResult(map, p, "/invalid", 5);
  }

  private static void populateMap(Map<Path, String> map, String prefix) {
    String dummyVal = "dummy";
    map.put(new Path(prefix + "/dirA/dirB/"), dummyVal);
    map.put(new Path(prefix + "/dirA/dirB/dirC"), dummyVal);
    map.put(new Path(prefix + "/dirA/dirB/dirC/file1"), dummyVal);
    map.put(new Path(prefix + "/dirA/dirB/dirC/file2"), dummyVal);
    map.put(new Path(prefix + "/dirA/file1"), dummyVal);
  }

  private static void assertClearResult(Map <Path, String> map,
      String prefixStr, String pathStr, int leftoverSize) {
    populateMap(map, prefixStr);
    LocalMetadataStore.clearHashByAncestor(new Path(prefixStr + pathStr), map);
    assertEquals(String.format("Map should have %d entries", leftoverSize),
        leftoverSize, map.size());
    map.clear();
  }

  @Override
  protected void verifyFileStatus(FileStatus status, long size) {
    super.verifyFileStatus(status, size);

    assertEquals("Replication value", REPLICATION, status.getReplication());
    assertEquals("Access time", getAccessTime(), status.getAccessTime());
    assertEquals("Owner", OWNER, status.getOwner());
    assertEquals("Group", GROUP, status.getGroup());
    assertEquals("Permission", PERMISSION, status.getPermission());
  }

  @Override
  protected void verifyDirStatus(FileStatus status) {
    super.verifyDirStatus(status);

    assertEquals("Mod time", getModTime(), status.getModificationTime());
    assertEquals("Replication value", REPLICATION, status.getReplication());
    assertEquals("Access time", getAccessTime(), status.getAccessTime());
    assertEquals("Owner", OWNER, status.getOwner());
    assertEquals("Group", GROUP, status.getGroup());
    assertEquals("Permission", PERMISSION, status.getPermission());
  }

}
