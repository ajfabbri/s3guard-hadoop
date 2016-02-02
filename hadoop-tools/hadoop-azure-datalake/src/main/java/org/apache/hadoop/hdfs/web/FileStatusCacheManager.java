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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Container to cache FileStatus instances mapped to Stream path. FileStatus instances cached for the configured
 * (core-site.xml) for specified duration. Any request for FileStatus information looks in to cache, if the instance is
 * available then the additional backend call is avoided. Each FileStatus cache has fixed life configured in core-site.xml.
 * once the FileStatus instance is expired, instance is removed from the cache.
 * <p>
 * Observation is GetFileStatus call is generously used to query on the file information across. This process level cache
 * helps to avoid load on the backend server. Application can configure the duration of the FileStatus instance to leave
 * in the cache using "ADL.Feature.Override.Cache.FileStatus.Duration" key in Core-Site.xml
 * <p>
 * ACID properties are maintained in overloaded api in @see PrivateAzureDataLakeFileSystem class.
 * <p>
 * Core-site.xml configuration
 * <p>
 * Example to update FileStatus cache duration
 * <pre>
 *     {@code
 *          <property>
 *              <name>ADL.Feature.Override.Cache.FileStatus.Duration</name>
 *              <value>5</value>
 *              <description>
 *                  Default timespan is 5 second to hold the object.
 *                  Valid only if ADL.Feature.Override.Cache.FileStatus is not configured to false.
 *              </description>
 *          </property>
 *     }
 * </pre>
 * <p>
 * Example to update FileStatus cache feature
 * <pre>
 *     {@code
 *     <property>
 *         <name>ADL.Feature.Override.Cache.FileStatus</name>
 *         <value>true</value>
 *         <description>
 *             Cache FileStatus object in memory available per process.
 *             Object is cached only for limited timestan or delete, rename, concat, create
 *             or createNonRecursive API is triggered on the same path. Default timestan is 5 second to hold the object
 *             and can be configured with ADL.Feature.Override.Cache.FileStatus.Duration.
 *
 *             If not configured default value is true.
 *         </description>
 *     </property>
 *     }
 * </pre>
 */
public final class FileStatusCacheManager {
    private static final FileStatusCacheManager FILE_STATUS_CACHE_MANAGER = new FileStatusCacheManager();
    private Map<String, FileStatusCacheObject> syncMap = null;

    /**
     * Constructor
     */
    private FileStatusCacheManager() {
        Map<String, FileStatusCacheObject> map = new HashMap<String, FileStatusCacheObject>();
        syncMap = Collections.synchronizedMap(map);
    }

    /**
     * Singleton instance ensure that cache manager is alive till the life of the process.
     *
     * @return FileStatusCacheManager instance
     */
    public static FileStatusCacheManager getInstance() {
        return FILE_STATUS_CACHE_MANAGER;
    }

    /**
     * Clear all the entries in the cache
     */
    public void clear() {
        syncMap.clear();
    }

    /**
     * Remove specific file stream instance from the cache.
     *
     * @param f Location of the file/folder
     */
    public void removeFileStatus(Path f) {
        if (f != null) {
            syncMap.remove(f.toString());
        }
    }

    /**
     * Retrieve FileStatus entry from the cache.
     *
     * @param path Location of the file/folder
     * @return Cached instance of the FileStatus instance, if not available then NULL.
     */
    public FileStatus getFileStatus(String path) {
        if (syncMap.containsKey(path)) {
            return syncMap.get(path).getFileStatus();
        }
        return null;
    }

    /**
     * Insert FileStatus instance. Key to access file status instance is created from the Path.
     *
     * @param fileStatus Valid FileStatus instance.
     */
    public void putFileStatus(FileStatus fileStatus) {
        if (fileStatus == null) {
            return;
        }

        if (FILE_STATUS_CACHE_MANAGER.syncMap.containsKey(fileStatus.getPath().toString())) {
            FILE_STATUS_CACHE_MANAGER.syncMap.remove(fileStatus.getPath().toString());
        }
        FILE_STATUS_CACHE_MANAGER.syncMap.put(fileStatus.getPath().toString(), new FileStatusCacheObject(fileStatus));
    }
}
