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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Use this class implementation to log debug information as part of the debug logs. Used during development.
 */
public class PrivateDebugAzureDataLakeFileSystem extends PrivateAzureDataLakeFileSystem {

    public static final Log LOG = LogFactory.getLog(PrivateAzureDataLakeFileSystem.class);
    private String objectGuid = "";

    public PrivateDebugAzureDataLakeFileSystem() {
        super();
        objectGuid = java.util.UUID.randomUUID().toString();
        ADLFeatureList.setLogEnable(true);
    }

    private Perf functionStart(String method, String message) {
        if (ADLFeatureList.isPerfEnabled()) {
            Perf perf = new Perf(method);
            perf.additionalPathInfo = message;
            return perf;
        }
        return null;
    }

    private void functionEnd(Perf perf, Object result) {
        if (ADLFeatureList.isPerfEnabled()) {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm:ss.SSS");
            Date startTime = new Date(perf.startTime);
            long endTimeInLong = System.currentTimeMillis();
            Date endTime = new Date(System.currentTimeMillis());
            StringBuilder obj = new StringBuilder();
            obj.append("{ \"Type\" : \"API\"");
            obj.append(",\"TotalTimeSpendInMs\" : " + (endTimeInLong - perf.startTime));
            obj.append(",\"Method\" : \"" + perf.methodName + "\"");
            obj.append(",\"jobid\" : \"" + perf.job + "\"");
            obj.append(",\"WebhdfsObjectId\" : \"" + objectGuid + "\"");
            obj.append(",\"Start\" : \"" + sdf.format(startTime) + "\"");
            obj.append(",\"End\" : \"" + sdf.format(endTime) + "\"");
            obj.append(",\"PathInfo\" : \"" + perf.additionalPathInfo + "\"");
            obj.append(",\"Result\" : \"" + ((result == null) ? "NA" : result.toString()) + "\"");
            obj.append(",\"LogVersion\" : \"" + ADLFeatureList.getLogVersion() + "\"");
            obj.append("}");
            ADLLogger.logPerf(obj.toString());
        }
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        Perf p = functionStart("OPEN", path.toString());
        FSDataInputStream in;
        try {
            in = super.open(path, i);
        } finally {
            functionEnd(p, null);
        }
        return in;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf
    ) throws IOException {
        super.initialize(uri, conf);
    }

    @Override
    public FSDataOutputStream create(final Path f, final FsPermission permission,
                                     final boolean overwrite, final int bufferSize, final short replication,
                                     final long blockSize, final Progressable progress) throws IOException {
        Perf p = functionStart("create", f.toString());
        FSDataOutputStream out;
        try {
            out = super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        } finally {
            functionEnd(p, null);
        }
        return out;
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        Perf p = functionStart("append", path.toString());
        FSDataOutputStream out = super.append(path, i, progressable);
        functionEnd(p, null);
        return out;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        Perf p = functionStart("rename", path.toString() + " -> " + path1.toString());
        Boolean out = null;
        try {
            out = super.rename(path, path1);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        Perf p = functionStart("delete", path.toString());
        Boolean out = null;
        try {
            out = super.delete(path, b);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        Perf p = functionStart("listStatus", path.toString());
        FileStatus[] out = null;
        try {
            out = super.listStatus(path);
        } finally {
            functionEnd(p, out);
        }
        return out;

    }

    @Override
    public void setWorkingDirectory(Path path) {
        Perf p = functionStart("setWorkingDirectory", "");
        try {
            super.setWorkingDirectory(path);
        } finally {
            functionEnd(p, null);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        Perf p = functionStart("mkdirs", path.toString());
        Boolean out = null;
        try {
            out = super.mkdirs(path, fsPermission);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        Perf p = functionStart("getFileStatus", path.toString());
        FileStatus out = null;
        try {
            out = super.getFileStatus(path);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public AclStatus getAclStatus(Path f) throws IOException {
        Perf p = functionStart("getAclStatus", f.toString());
        AclStatus out = null;
        try {
            out = super.getAclStatus(f);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public ContentSummary getContentSummary(final Path f) throws IOException {
        Perf p = functionStart("getContentSummary", f.toString());
        ContentSummary out = null;
        try {
            out = super.getContentSummary(f);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus status, long offset, long length) throws IOException {
        Perf p = functionStart("getFileBlockLocations", status.toString() + " Offset " + offset + " length " + length);
        BlockLocation[] out = null;
        try {
            out = super.getFileBlockLocations(status, offset, length);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(final Path f, long offset, long length) throws IOException {
        Perf p = functionStart("getFileBlockLocations", f.toString() + " Offset " + offset + " length " + length);
        BlockLocation[] out = null;
        try {
            out = super.getFileBlockLocations(f, offset, length);
        } finally {
            functionEnd(p, out);
        }
        return out;
    }

    class Perf {
        private long startTime;
        private String methodName;
        private String job;
        private String additionalPathInfo;

        Perf(String invokedMethodName) {
            startTime = System.currentTimeMillis();
            this.methodName = invokedMethodName;
            job = getConf().get("mapreduce.job.dir", "NA");
        }
    }
}
