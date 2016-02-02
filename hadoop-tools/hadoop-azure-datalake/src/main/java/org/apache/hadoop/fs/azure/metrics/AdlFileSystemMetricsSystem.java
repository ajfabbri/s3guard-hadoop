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

import org.apache.hadoop.hdfs.web.ADLLogger;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

/**
 * Expose functions to instrument performance logging in ADL FileSystem implementation.
 */
public final class AdlFileSystemMetricsSystem {

    private static MetricsSystemImpl instance;

    private AdlFileSystemMetricsSystem() {
    }

    public static synchronized void fileSystemStarted() {
        if (instance == null) {
            instance = new MetricsSystemImpl();
            instance.init("adl-file-system");
        }
        ADLLogger.log("Inside adl-file-system metrics");
    }

    public static synchronized void fileSystemClosed() {
        if (instance != null) {
            instance.stop();
            instance.shutdown();
            instance = null;
        }
    }

    public static void registerSource(String name, String desc,
                                      MetricsSource source) {
        // caller has to use unique name to register source
        if (instance != null) {
            instance.register(name, desc, source);
            ADLLogger.log(
                    "Inside register adl-file-system metrics");
        }
    }

    public static synchronized void unregisterSource(String name) {
        if (instance != null) {
            instance.unregisterSource(name);
        }
    }

}
