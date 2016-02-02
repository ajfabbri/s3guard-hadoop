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

final class ADLConfKeys {
    public static final String ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_MAX_CONCURRENT_CONNECTION =
            "ADL.Feature.Override.ReadAhead.MAX.Concurrent.Connection";
    public static final int ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_MAX_CONCURRENT_CONNECTION_DEFAULT = 1;
    public static final String ADL_EVENTS_TRACKING_SOURCE = "adl.events.tracking.source";
    public static final String ADL_EVENTS_TRACKING_CLUSTERNAME = "adl.events.tracking.clustername";
    public static final String ADL_TRACKING_JOB_ID = "adl.tracking.job.id";
    public static final int ADL_DEFAULT_CACHE_CURATION_TIME = 5;
    static final String ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER = "adl.Debug.Override.LocalUserAsFileOwner";
    static final boolean ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT = false;
    static final String ADL_FEATURE_REDIRECT_OFF = "ADL.Feature.Override.Redirection.Off";
    static final boolean ADL_FEATURE_REDIRECT_OFF_DEFAULT = true;
    static final String ADL_FEATURE_CACHE_FILE_STATUS = "ADL.Feature.Override.Cache.FileStatus";
    static final boolean ADL_FEATURE_CACHE_FILE_STATUS_DEFAULT = true;
    static final String ADL_FEATURE_CACHE_FILE_STATUS_DURATION = "ADL.Feature.Override.Cache.FileStatus.Duration";
    static final int ADL_FEATURE_CACHE_FILE_STATUS_DURATION_DEFAULT = ADL_DEFAULT_CACHE_CURATION_TIME; // In seconds
    static final String ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED = "ADL.Feature.Override.GetBlockLocation.Locally.Bundled";
    static final boolean ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED_DEFAULT = true;
    static final String ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD = "ADL.Feature.Override.ReadAhead";
    static final boolean ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_DEFAULT = true;
    static final String ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE = "ADL.Feature.Override.ReadAhead.Max.BufferSize";
    static final int KB = 1024;
    static final int MB = KB * KB;
    static final int DEFAULT_BLOCK_SIZE = 4 * MB;
    static final int DEFAULT_EXTENT_SIZE = 256 * MB;
    static final int DEFAULT_TIMEOUT_IN_SECONDS = 120;
    static final int MILLISECONDS = 1000;
    static final int ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE_DEFAULT = DEFAULT_BLOCK_SIZE;

    private ADLConfKeys() {
    }

}
