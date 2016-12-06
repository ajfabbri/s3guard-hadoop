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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

/**
 * {@code MetadataStore} defines the set of operations that any metadata store
 * implementation must provide.  Note that all {@link Path} objects provided
 * to methods must be absolute, not relative paths.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MetadataStore extends Closeable {

  /**
   * Performs one-time initialization of the metadata store.
   *
   * @param fs {@code FileSystem} associated with the MetadataStore
   * @throws IOException if there is an error
   */
  void initialize(FileSystem fs) throws IOException;

  /**
   * Deletes exactly one path.
   *
   * @param path the path to delete
   * @throws IOException if there is an error
   */
  void delete(Path path) throws IOException;

  /**
   * Deletes the entire sub-tree rooted at the given path.
   *
   * In addition to affecting future calls to {@link #get(Path)},
   * implementations must also update any stored {@code DirListingMetadata}
   * objects which track the parent of this file.
   *
   * @param path the root of the sub-tree to delete
   * @throws IOException if there is an error
   */
  void deleteSubtree(Path path) throws IOException;

  /**
   * Gets metadata for a path.
   *
   * @param path the path to get
   * @return metadata for {@code path}, {@code null} if not found
   * @throws IOException if there is an error
   */
  PathMetadata get(Path path) throws IOException;

  /**
   * Lists metadata for all direct children of a path.
   *
   * @param path the path to list
   * @return metadata for all direct children of {@code path} which are being
   *     tracked by the MetadataStore, or {@code null} if the path was not found
   *     in the MetadataStore.
   * @throws IOException if there is an error
   */
  DirListingMetadata listChildren(Path path) throws IOException;

  /**
   * Record the effects of a {@link FileSystem#rename(Path, Path)} in the
   * MetadataStore.  Clients provide explicit enumeration of the affected
   * paths (recursively), before and after the rename.
   *
   * This operation is not atomic, unless specific implementations claim
   * otherwise.
   *
   * On the need to provide an enumeration of directory trees instead of just
   * source and destination paths:
   * Since a MetadataStore does not have to track all metadata for the
   * underlying storage system, and a new MetadataStore may be created on an
   * existing underlying filesystem, this move() may be the first time the
   * MetadataStore sees the affected paths.  Therefore, simply providing src
   * and destination paths may not be enough to record the deletions (under
   * src path) and creations (at destination) that are happening during the
   * rename().
   *
   * @param pathsToDelete Collection of all paths that were removed from the
   *                      source directory tree of the move.
   * @param pathsToCreate Collection of all PathMetadata for the new paths
   *                      that were created at the destination of the rename
   *                      ().
   * @throws IOException if there is an error
   */
  void move(Collection<Path> pathsToDelete, Collection<PathMetadata>
      pathsToCreate) throws IOException;

  /**
   * Saves metadata for exactly one path.  For a deeply nested path, this method
   * will not automatically create the full ancestry.  Callers need to ensure
   * saving the full path ancestry.
   *
   * Implementations must update any {@code DirListingMetadata} objects which
   * track the immediate parent of this file.
   *
   * @param meta the metadata to save
   * @throws IOException if there is an error
   */
  void put(PathMetadata meta) throws IOException;

  /**
   * Save directory listing metadata. Callers may save a partial directory
   * listing for a given path, or may store a complete and authoritative copy
   * of the directory listing.  {@code MetadataStore} implementations may
   * subsequently keep track of all modifications to the directory contents at
   * this path, and return authoritative results from subsequent calls to
   * {@link #listChildren(Path)}. See {@link DirListingMetadata}.
   *
   * Any authoritative results returned are only authoritative for the scope
   * of the {@code MetadataStore}:  A per-process {@code MetadataStore}, for
   * example, would only show results visible to that process, potentially
   * missing metadata updates (create, delete) made to the same path by
   * another process.
   *
   * @param meta Directory listing metadata.
   * @throws IOException if there is an error
   */
  void put(DirListingMetadata meta) throws IOException;

  /**
   * Destroy all resources associated with the metadata store.
   *
   * The destroyed resources can be DynamoDB tables, MySQL databases/tables, or
   * HDFS directories. Any operations after calling this method may possibly
   * fail.
   *
   * This operation is idempotent.
   *
   * @throws IOException if there is an error
   */
  void destroy() throws IOException;

  /** Optional: log full state to given log at debug level. */
  default void logStateDebug(Logger log) throws IOException {
    log.debug("not implemented");
  }
}
