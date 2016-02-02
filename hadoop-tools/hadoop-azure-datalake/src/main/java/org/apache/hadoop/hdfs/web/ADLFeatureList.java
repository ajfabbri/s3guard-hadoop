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

/**
 * Expose constants for supported features in ADL.
 */
public final class ADLFeatureList {
    public static final String ADL_WEBSDK_VERSION_KEY = "ADLLibVersion";
    private static final String LOG_VERSION = "1.2.1";
    private static boolean featureCacheFileStatus = true;
    private static int featureCacheFileStatusDuration = ADLConfKeys.ADL_DEFAULT_CACHE_CURATION_TIME; // In Seconds
    private static boolean featureGetBlockLocationLocallyBundled = true;
    private static boolean featureFlushWhenEOF = true;
    private static boolean featureOffsetSpecification = true;
    private static boolean featureConcurrentReadWithReadAhead = true;
    /**
     * This is temporary flag. And would be removed when migrating to ADL jar
     */
    private static boolean logEnable = false;
    private static boolean perfEnable = false;
    private static boolean featureRedirectOff = true;

    private ADLFeatureList() {

    }

    public static boolean isFeatureCacheFileStatus() {
        return featureCacheFileStatus;
    }

    public static void setFeatureCacheFileStatus(boolean featureCacheFileStatus) {
        ADLFeatureList.featureCacheFileStatus = featureCacheFileStatus;
    }

    public static int getFeatureCacheFileStatusDuration() {
        return featureCacheFileStatusDuration;
    }

    public static void setFeatureCacheFileStatusDuration(int featureCacheFileStatusDuration) {
        ADLFeatureList.featureCacheFileStatusDuration = featureCacheFileStatusDuration;
    }

    public static boolean isFeatureGetBlockLocationLocallyBundled() {
        return featureGetBlockLocationLocallyBundled;
    }

    public static void setFeatureGetBlockLocationLocallyBundled(boolean featureGetBlockLocationLocallyBundled) {
        ADLFeatureList.featureGetBlockLocationLocallyBundled = featureGetBlockLocationLocallyBundled;
    }

    public static boolean isFeatureFlushWhenEOF() {
        return featureFlushWhenEOF;
    }

    public static void setFeatureFlushWhenEOF(boolean featureFlushWhenEOF) {
        ADLFeatureList.featureFlushWhenEOF = featureFlushWhenEOF;
    }

    public static boolean isFeatureOffsetSpecification() {
        return featureOffsetSpecification;
    }

    public static void setFeatureOffsetSpecification(boolean featureOffsetSpecification) {
        ADLFeatureList.featureOffsetSpecification = featureOffsetSpecification;
    }

    public static boolean isFeatureConcurrentReadWithReadAhead() {
        return featureConcurrentReadWithReadAhead;
    }

    public static void setFeatureConcurrentReadWithReadAhead(boolean featureConcurrentReadWithReadAhead) {
        ADLFeatureList.featureConcurrentReadWithReadAhead = featureConcurrentReadWithReadAhead;
    }

    public static String getLogVersion() {
        return LOG_VERSION;
    }

    public static boolean isLogEnabled() {
        return logEnable;
    }

    public static void setLogEnable(boolean le) {
        logEnable = le;
    }

    public static boolean isPerfEnabled() {
        return perfEnable;
    }

    public static void setPerfEnable(boolean enable) {
        ADLFeatureList.perfEnable = enable;
    }

    public static boolean isFeatureRedirectOff() {
        return featureRedirectOff;
    }

    public static void setFeatureRedirectOff(boolean f) {
        featureRedirectOff = f;
    }

    public static String getADLVersion() {
        int version = 0;
        if (isFeatureRedirectOff()) {
            version = 1;
        }

        if (featureGetBlockLocationLocallyBundled) {
            version += 2;
        }

        if (featureCacheFileStatus) {
            version += 3;
        }

        if (featureFlushWhenEOF) {
            version += 4;
        }

        if (featureOffsetSpecification) {
            version += 5;
        }

        if (featureConcurrentReadWithReadAhead) {
            version += 6;
        }

        return "V" + version;
    }
}
