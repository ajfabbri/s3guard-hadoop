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

package org.apache.hadoop.fs.azure.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Capture performance metric for ADL filesystem.
 */
@Metrics(about = "Metrics for ADL", context = "adlFileSystem")
public class AdlFileSystemInstrumentation implements MetricsSource {
    public static final String METRIC_TAG_FILESYSTEM_ID = "adlFileSystemId";
    public static final String ADL_WEB_RESPONSES = "adl_web_responses";
    public static final String ADL_BYTES_WRITTEN = "adl_bytes_written_last_second";
    public static final String ADL_BYTES_READ = "adl_bytes_read_last_second";
    public static final String ADL_RAW_BYTES_UPLOADED = "adl_raw_bytes_uploaded";
    public static final String ADL_RAW_BYTES_DOWNLOADED = "adl_raw_bytes_downloaded";
    public static final String ADL_FILES_CREATED = "adl_files_created";
    public static final String ADL_FILES_DELETED = "adl_files_deleted";
    public static final String ADL_DIRECTORIES_CREATED = "adl_directories_created";
    public static final String ADL_DIRECTORIES_DELETED = "adl_directories_deleted";
    public static final String ADL_UPLOAD_RATE = "adl_maximum_upload_bytes_per_second";
    public static final String ADL_DOWNLOAD_RATE = "adl_maximum_download_bytes_per_second";
    public static final String ADL_UPLOAD_LATENCY = "adl_average_block_upload_latency_ms";
    public static final String ADL_DOWNLOAD_LATENCY = "adl_average_block_download_latency_ms";
    public static final String ADL_CLIENT_ERRORS = "adl_client_errors";
    public static final String ADL_SERVER_ERRORS = "adl_server_errors";
    private final MetricsRegistry registry = new MetricsRegistry("adlFileSystem").setContext("adlFileSystem");
    private final MutableCounterLong numberOfWebResponses =
            registry.newCounter(ADL_WEB_RESPONSES, "Total number of web responses obtained from ADL", 0L);
    private final MutableCounterLong numberOfFilesCreated =
            registry.newCounter(ADL_FILES_CREATED, "Total number of files created through the ADL file system.", 0L);
    private final MutableCounterLong numberOfFilesDeleted =
            registry.newCounter(ADL_FILES_DELETED, "Total number of files deleted through the ADL file system.", 0L);
    private final MutableCounterLong numberOfDirectoriesCreated =
            registry.newCounter(ADL_DIRECTORIES_CREATED,
                    "Total number of directories created through the ADL file system.", 0L);
    private final MutableCounterLong numberOfDirectoriesDeleted =
            registry.newCounter(ADL_DIRECTORIES_DELETED,
                    "Total number of directories deleted through the ADL file system.", 0L);
    private final MutableGaugeLong bytesWrittenInLastSecond =
            registry.newGauge(ADL_BYTES_WRITTEN,
                    "Total number of bytes written to ADL during the last second.", 0L);
    private final MutableGaugeLong bytesReadInLastSecond =
            registry.newGauge(ADL_BYTES_READ,
                    "Total number of bytes read from ADL during the last second.", 0L);
    private final MutableGaugeLong maximumUploadBytesPerSecond =
            registry.newGauge(ADL_UPLOAD_RATE,
                    "The maximum upload rate encountered to ADL in bytes/second.", 0L);
    private final MutableGaugeLong maximumDownloadBytesPerSecond =
            registry.newGauge(ADL_DOWNLOAD_RATE,
                    "The maximum download rate encountered to ADL in bytes/second.", 0L);
    private final MutableCounterLong rawBytesUploaded =
            registry.newCounter(ADL_RAW_BYTES_UPLOADED,
                    "Total number of raw bytes (including overhead) uploaded to Azure" + " Storage.", 0L);
    private final MutableCounterLong rawBytesDownloaded =
            registry.newCounter(ADL_RAW_BYTES_DOWNLOADED,
                    "Total number of raw bytes (including overhead) downloaded from Azure" + " Storage.", 0L);
    private final MutableCounterLong clientErrors =
            registry.newCounter(ADL_CLIENT_ERRORS,
                    "Total number of client-side errors by ADL (excluding 404).", 0L);
    private final MutableCounterLong serverErrors =
            registry.newCounter(ADL_SERVER_ERRORS, "Total number of server-caused errors by ADL.", 0L);
    private UUID fileSystemInstanceId;
    private AtomicLong inMemoryNumberOfWebResponses = new AtomicLong(0);

    public AdlFileSystemInstrumentation(Configuration conf) {
        fileSystemInstanceId = UUID.randomUUID();
        registry.tag("adlFileSystemId", "A unique identifier for the file ",
                fileSystemInstanceId.toString());
    }

    /**
     * The unique identifier for this file system in the metrics.
     * @return Unique id for initialized current instance
     */
    public final UUID getFileSystemInstanceId() {
        return fileSystemInstanceId;
    }

    /**
     * Get the metrics registry information.
     *
     * @return metrics info
     */
    public final MetricsInfo getMetricsRegistryInfo() {
        return registry.info();
    }

    /**
     * Sets the account name to tag all the metrics with.
     *
     * @param accountName The account name.
     */
    public final void setAccountName(String accountName) {
        registry.tag("accountName",
                "Name of the ADL account that these metrics are going against",
                accountName);
    }

    /**
     * Sets the container name to tag all the metrics with.
     *
     * @param containerName The container name.
     */
    public final void setContainerName(String containerName) {
        registry.tag(
                "containerName",
                "Name of the ADL container that these metrics are going against",
                containerName);
    }

    /**
     * Indicate that we just got a web response from ADL. This should be called
     * for every web request/response we do (to get accurate metrics of how
     * we're hitting the storage service).
     */
    public final void webResponse() {
        numberOfWebResponses.incr();
        inMemoryNumberOfWebResponses.incrementAndGet();
    }

    /**
     * Gets the current number of web responses obtained from ADL.
     *
     * @return The number of web responses.
     */
    public final long getCurrentWebResponses() {
        return inMemoryNumberOfWebResponses.get();
    }

    /**
     * Indicate that we just created a file through ADL.
     */
    public final void fileCreated() {
        numberOfFilesCreated.incr();
    }

    /**
     * Indicate that we just deleted a file through ADL.
     */
    public final void fileDeleted() {
        numberOfFilesDeleted.incr();
    }

    /**
     * Indicate that we just created a directory through ADL.
     */
    public final void directoryCreated() {
        numberOfDirectoriesCreated.incr();
    }

    /**
     * Indicate that we just deleted a directory through ADL.
     */
    public final void directoryDeleted() {
        numberOfDirectoriesDeleted.incr();
    }

    /**
     * Sets the current gauge value for how many bytes were written in the last
     * second.
     *
     * @param currentBytesWritten The number of bytes.
     */
    public final void updateBytesWrittenInLastSecond(long currentBytesWritten) {
        bytesWrittenInLastSecond.set(currentBytesWritten);
    }

    /**
     * Sets the current gauge value for how many bytes were read in the last
     * second.
     *
     * @param currentBytesRead The number of bytes.
     */
    public final void updateBytesReadInLastSecond(long currentBytesRead) {
        bytesReadInLastSecond.set(currentBytesRead);
    }

    /**
     * Indicate that we just uploaded some data to ADL.
     *
     * @param numberOfBytes The raw number of bytes uploaded (including overhead).
     */
    public final void rawBytesUploaded(long numberOfBytes) {
        rawBytesUploaded.incr(numberOfBytes);
    }

    /**
     * Indicate that we just downloaded some data to ADL.
     *
     * @param numberOfBytes The raw number of bytes downloaded (including overhead).
     */
    public final void rawBytesDownloaded(long numberOfBytes) {
        rawBytesDownloaded.incr(numberOfBytes);
    }

    /**
     * Indicate that we just encountered a client-side error.
     */
    public final void clientErrorEncountered() {
        clientErrors.incr();
    }

    /**
     * Indicate that we just encountered a server-caused error.
     */
    public final void serverErrorEncountered() {
        serverErrors.incr();
    }

    @Override
    public final void getMetrics(MetricsCollector collector, boolean all) {
        registry.snapshot(collector.addRecord(registry.info()), true);
    }
}
