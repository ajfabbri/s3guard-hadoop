/**
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * This is a local, in-memory, implementation of MetadataStore.
 * This is *not* a coherent cache across processes.  It is only
 * locally-coherent.
 *
 * The purpose of this is for unit testing.  It could also be used to accelerate
 * local-only operations where only one process is operating on a given object
 * store, or multiple processes are accessing a read-only storage bucket.
 *
 * This MetadataStore does not enforce filesystem rules such as disallowing
 * non-recursive removal of non-empty directories.  It is assumed the caller
 * already has to perform these sorts of checks.
 */
public class LocalMetadataStore extends MetadataStore {

  public static final int DEFAULT_MAX_RECORDS = 128;
  public static final String CONF_MAX_RECORDS =
      "fs.metadatastore.local.max_records";

  /** Contains directories and files. */
  private LruHashMap<Path,PathMetadata> fileHash;

  /** Contains directory listings. */
  private LruHashMap<Path,DirListingMetadata> dirHash;

  @Override
  public void initialize(Configuration conf) throws IOException {
    // TODO implement more configuration vars
    int maxRecords = conf.getInt(CONF_MAX_RECORDS, DEFAULT_MAX_RECORDS);
    if (maxRecords < 4)
      maxRecords = 4;
    // Start w/ less than max capacity.  Space / time trade off.
    fileHash = new LruHashMap<>(maxRecords/2, maxRecords);
    dirHash = new LruHashMap<>(maxRecords/4, maxRecords);
  }

  @Override
  public void delete(Path path) throws IOException {
    doDelete(path, false);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    doDelete(path, true);
  }

  private void doDelete(Path path, boolean recursive) {

    // We could implement positive hit for 'deleted' files.  For now we
    // do not track them.

    // Delete entry from file cache, then from cached parent directory, if any

    synchronized (this) {
      // Remove target file/dir
      fileHash.remove(path);

      // Update parent dir listing, if any
      dirHashDeleteFile(path);

      if (recursive) {
        // Remove all entries that have this dir as path prefix.
        clearHashByAncestor(path, dirHash);
        clearHashByAncestor(path, fileHash);
      }
    }
  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    PathMetadata m;
    synchronized (this) {
      m = fileHash.mruGet(path);
    }
    return m;
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    DirListingMetadata m;
    synchronized (this) {
      m = dirHash.mruGet(path);
    }
    return m;
  }

  @Override
  public void move(Path src, Path dst) throws IOException {
    // TODO implement me
    throw new IOException("LocalMetadataStore#move() not implemented yet");
  }

  @Override
  public void put(PathMetadata meta) throws IOException {

    FileStatus status = meta.getFileStatus();
    Path path = status.getPath();
    synchronized (this) {

      /* Add entry for this file. */
      fileHash.put(path, meta);

      /* If directory, go ahead and cache the fact that it is empty. */
      if (status.isDirectory()) {
        DirListingMetadata dir = new DirListingMetadata(path,
            DirListingMetadata.EMPTY_DIR, true);
        dirHash.put(path, dir);
      }

      /* Update cached parent dir. */
      Path parentPath = path.getParent();
      DirListingMetadata parent = dirHash.get(parentPath);
      if (parent == null) {
        /* Track this new file's listing in parent.  Parent is not
         * authoritative, since there may be other items in it we don't know
         * about. */
        parent = new DirListingMetadata(parentPath,
            DirListingMetadata.EMPTY_DIR, false);
        dirHash.put(parentPath, parent);
      }
      parent.put(status);
    }
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
    synchronized (this) {
      dirHash.put(meta.getPath(), meta);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @VisibleForTesting
  static <T> void clearHashByAncestor(Path ancestor, Map<Path,T> hash) {
    for (Iterator<Map.Entry<Path,T>> it = hash.entrySet().iterator();
         it.hasNext(); )
    {
      Map.Entry<Path,T> entry = it.next();
      Path f = entry.getKey();
      if (isAncestorOf(ancestor, f)) {
        it.remove();
      }
    }
  }

  /**
   * @return true iff 'ancestor' is ancestor dir in path 'f'.
   * All paths here are absolute.  Dir does not count as its own ancestor.
   */
  private static boolean isAncestorOf(Path ancestor, Path f) {
    String aStr = ancestor.toString();
    if (! ancestor.isRoot())
      aStr += "/";
    String fStr = f.toString();
    return (fStr.indexOf(aStr) == 0);
  }

  /**
   * Update dirHash to reflect deletion of file 'f'.  Call with lock held.
   */
  private void dirHashDeleteFile(Path path) {
    Path parent = path.getParent();
    if (parent != null) {
      DirListingMetadata dir = dirHash.get(parent);
      if (dir != null) {
        dir.remove(path);
      }
    }
  }
}
