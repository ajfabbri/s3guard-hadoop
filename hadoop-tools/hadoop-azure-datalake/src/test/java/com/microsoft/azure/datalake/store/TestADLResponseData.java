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

package com.microsoft.azure.datalake.store;

import org.apache.hadoop.fs.FileStatus;

public class TestADLResponseData {

    public static String getGetFileStatusJSONResponse(FileStatus status) {
        String str = "{\"FileStatus\":{\"length\":"+status.getLen()+",\"pathSuffix\":\"\",\"type\":\""+ (status.isDirectory() ? "DIRECTORY" : "FILE")+"\"" +
                ",\"blockSize\":"+status.getBlockSize()+",\"accessTime\":"+status.getAccessTime()+",\"modificationTime\":"+status.getModificationTime()+"" +
                ",\"replication\":"+status.getReplication()+",\"permission\":\""+status.getPermission()+"\",\"owner\":\""+status.getOwner()+"\",\"group\":\""+status.getGroup()+"\"}}";

        return str;
    }

    public static String getGetFileStatusJSONResponse() {
        String str = "{\"FileStatus\":{\"length\":0,\"pathSuffix\":\"\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1452103827023,\"modificationTime\":1452103827023,\"replication\":0,\"permission\":\"777\",\"owner\":\"NotSupportYet\",\"group\":\"NotSupportYet\"}}";
        return str;
    }


    public static String getListFileStatusJSONResponse(int dirSize) {
        String list="";
        for(int i=0;i<dirSize;++i) {
            list += "{\"length\":1024,\"pathSuffix\":\""+java.util.UUID.randomUUID()+"\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1452103878833,\"modificationTime\":1452103879190,\"replication\":0,\"permission\":\"777\",\"owner\":\"NotSupportYet\",\"group\":\"NotSupportYet\"},";
        }

        list = list.substring(0,list.length()-1);
        String str = "{\"FileStatuses\":{\"FileStatus\":["+list+"]}}";

        return str;
    }

    public static String getJSONResponse(boolean status) {
        String str = "{\"boolean\":"+status+"}";
        return str;
    }

    public static String getErrorIllegalArgumentExceptionJSONResponse() {
        String str = "{\n" +
                "  \"RemoteException\":\n" +
                "  {\n" +
                "    \"exception\"    : \"IllegalArgumentException\",\n" +
                "    \"javaClassName\": \"java.lang.IllegalArgumentException\",\n" +
                "    \"message\"      : \"IllegalArgumentException" +
                "  }\n" +
                "}";

        return str;
    }

    public static String getErrorInternalServerExceptionJSONResponse() {
        String str = "{\n" +
                "  \"RemoteException\":\n" +
                "  {\n" +
                "    \"exception\"    : \"RumtimeException\",\n" +
                "    \"javaClassName\": \"java.lang.RumtimeException\",\n" +
                "    \"message\"      : \"Internal Server Error" +
                "  }\n" +
                "}";

        return str;
    }

}
