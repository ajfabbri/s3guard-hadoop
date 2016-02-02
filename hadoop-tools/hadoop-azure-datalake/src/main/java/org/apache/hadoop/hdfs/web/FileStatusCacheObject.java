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

import java.util.Timer;
import java.util.TimerTask;

/**
 * @see FileStatusCacheManager
 * <p>
 * This class holds FileStatus instance with timer associated with instance. Expiry time is configured as part
 * of the Core-site.xml.
 */
final class FileStatusCacheObject {
    private FileStatus fileStatus;

    /**
     * Constructor
     *
     * @param fileStatusObject valid FileStatus object
     */
    FileStatusCacheObject(FileStatus fileStatusObject) {
        new ExpiryNotification(ADLFeatureList.getFeatureCacheFileStatusDuration());
        this.fileStatus = fileStatusObject;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    private void objectExpired() {
        ADLLogger.log("Removed from cache : " + fileStatus.getPath().toString());
        FileStatusCacheManager.getInstance().removeFileStatus(new Path(this.fileStatus.getPath().toString()));
    }

    class ExpiryNotification {
        private Timer timer;

        public ExpiryNotification(int seconds) {
            timer = new Timer();
            timer.schedule(new RemindTask(), seconds * ADLConfKeys.MILLISECONDS);
        }

        class RemindTask extends TimerTask {
            public void run() {
                objectExpired();
                timer.cancel(); //Terminate the timer thread
            }
        }
    }
}
