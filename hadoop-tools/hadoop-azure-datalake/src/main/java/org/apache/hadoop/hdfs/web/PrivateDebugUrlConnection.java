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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.security.Permission;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extended version HttpUrlConnection allows request to be monitor for
 * performance and instrument with tracking header
 * part of the request.
 */
public final class PrivateDebugUrlConnection extends HttpURLConnection {

  private String guid;
  private long startTime;
  private long connectionEstablishedTime;
  private long endTimeInLong;
  private long openConnectionStartTime;
  private long openConnectionEndTime;
  private HttpURLConnection another;
  private InputStream in = null;

  /**
   * Constructs a URL connection to the specified URL. A connection to
   * the object referenced by the URL is not created.
   *
   * @param url                 the specified URL.
   * @param uniqueId            Unique id per webhdfs file system instance.
   *                            For debugging/performance purpose
   * @param connectionStartTime Connection initiate start time from
   *                            ConnectionFactory. For debugging/performance
   *                            purpose
   * @param connectionEndTime   Connection initiate end time from
   *                            ConnectionFactory. For debugging/performance
   *                            purpose
   */
  protected PrivateDebugUrlConnection(URL url, String uniqueId,
      long connectionStartTime, long connectionEndTime) {
    super(url);
    this.guid = uniqueId;
    this.openConnectionEndTime = connectionEndTime;
    this.openConnectionStartTime = connectionStartTime;
  }

  public static Map<String, String> getQueryMap(String query) {
    String[] params = query.split("&");
    Map<String, String> map = new HashMap<String, String>();
    for (String param : params) {
      String name = param.split("=")[0];
      String value = param.split("=")[1];
      map.put(name, value);
    }
    return map;
  }

  private void debugInfo() {
    if (!ADLLogger.isPerfEnabled()) {
      return;
    }
    endTimeInLong = System.currentTimeMillis();
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy HH:mm:ss.SSS");
    Date startTimeDate = new Date(startTime);
    Date connectionEstablishedTimeDate = new Date(connectionEstablishedTime);

    int responseCode = 0;
    String responseMessage = "NA";
    try {
      responseCode = getResponseCode();
      responseMessage = getResponseMessage();
    } catch (IOException e) {
      // Suppress exception.
    }

    StringBuilder obj = new StringBuilder();
    obj.append("{ \"Type\" : \"Network\"");
    obj.append(",\"TotalTimeSpendInMs\" : ").append(endTimeInLong - startTime);
    obj.append(",\"ResponseCode\" : ").append(responseCode);
    obj.append(",\"Response\" : \"").append(responseMessage).append("\"");
    obj.append(",\"ServerPerf\" : \"").append(getHeaderField("server-perf"))
        .append("\"");
    obj.append(",\"Machine\" : \"").append(getHeaderField("x-origin-server"))
        .append("\"");
    obj.append(",\"RequestStartTime\" : \"").append(sdf.format(startTimeDate))
        .append("\"");
    obj.append(",\"SSLHandshakeCompleteTime\" : \"")
        .append(sdf.format(connectionEstablishedTimeDate)).append("\"");
    obj.append(",\"SSLHandshakeCompleteTimeInMs\" : ")
        .append(connectionEstablishedTime - startTime);
    obj.append(",\"RequestEndTime\" : \"")
        .append(sdf.format(new Date(endTimeInLong))).append("\"");
    obj.append(",\"URL\" :  \"").append(getURL()).append("\"");
    obj.append(",\"WebhdfsObjectId\" : \"").append(guid).append("\"");
    obj.append(",\"OAuthTokenRequest\" : \"")
        .append(sdf.format(openConnectionStartTime)).append("\"");
    obj.append(",\"OAuthTokenResponse\" : \"")
        .append(sdf.format(openConnectionEndTime)).append("\"");
    obj.append(",\"OAuthRefreshTokenTimeInMs\" : ")
        .append(openConnectionEndTime - openConnectionStartTime);
    obj.append(",\"Host\" : \"")
        .append(PrivateAzureDataLakeFileSystem.getMachineName()).append("\"");
    obj.append(",\"LogVersion\" : \"").append(ADLConfKeys.LOG_VERSION)
        .append("\"");
    obj.append("}");
    ADLLogger.logPerf(obj.toString());
  }

  public void setAnotherUrlConnection(HttpURLConnection urlConnection) {
    this.another = urlConnection;
  }

  @Override
  public void connect() throws IOException {
    startTime = System.currentTimeMillis();
    another.connect();
    connectionEstablishedTime = System.currentTimeMillis();
  }

  @Override
  public InputStream getErrorStream() {
    debugInfo();
    in = another.getErrorStream();
    return in;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    in = another.getInputStream();
    return in;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return another.getOutputStream();
  }

  @Override
  public int getConnectTimeout() {
    return another.getConnectTimeout();
  }

  @Override
  public void setConnectTimeout(int timeout) {
    another.setConnectTimeout(timeout);
  }

  @Override
  public int getReadTimeout() {
    return another.getReadTimeout();
  }

  @Override
  public void setReadTimeout(int timeout) {
    another.setReadTimeout(timeout);
  }

  @Override
  public String getContentType() {
    return another.getContentType();
  }

  @Override
  public String getContentEncoding() {
    return another.getContentEncoding();
  }

  @Override
  public long getExpiration() {
    return another.getExpiration();
  }

  @Override
  public long getLastModified() {
    return another.getLastModified();
  }

  @Override
  public String getHeaderField(String name) {
    String header = another.getHeaderField(name);
    if (header == null) {
      header = "";
    }
    return header;
  }

  @Override
  public Map<String, List<String>> getHeaderFields() {
    return another.getHeaderFields();
  }

  @Override
  public int getHeaderFieldInt(String name, int defaultValue) {
    return another.getHeaderFieldInt(name, defaultValue);
  }

  @Override
  public long getHeaderFieldDate(String name, long defaultValue) {
    return another.getHeaderFieldDate(name, defaultValue);
  }

  @Override
  public void disconnect() {
    debugInfo();
    if (in != null) {
      try {
        in.close();
      } catch (IOException e) {
      }
    }
    another.disconnect();
  }

  @Override
  public boolean usingProxy() {
    return another.usingProxy();
  }

  @Override
  public String getHeaderFieldKey(int n) {
    return another.getHeaderFieldKey(n);
  }

  @Override
  public String getHeaderField(int n) {
    return another.getHeaderField(n);
  }

  @Override
  public Object getContent() throws IOException {
    return another.getContent();
  }

  @Override
  public Object getContent(Class[] classes) throws IOException {
    return another.getContent(classes);
  }

  @Override
  public Permission getPermission() throws IOException {
    return another.getPermission();
  }

  @Override
  public boolean getDoInput() {
    return another.getDoInput();
  }

  @Override
  public void setDoInput(boolean doinput) {
    another.setDoInput(doInput);
  }

  @Override
  public boolean getDoOutput() {
    return another.getDoOutput();
  }

  @Override
  public void setDoOutput(boolean dooutput) {
    another.setDoOutput(dooutput);
  }

  @Override
  public boolean getAllowUserInteraction() {
    return another.getAllowUserInteraction();
  }

  @Override
  public void setAllowUserInteraction(boolean allowuserinteraction) {
    another.setAllowUserInteraction(allowuserinteraction);
  }

  @Override
  public boolean getUseCaches() {
    return another.getUseCaches();
  }

  @Override
  public void setUseCaches(boolean usecaches) {
    another.setUseCaches(usecaches);
  }

  @Override
  public long getIfModifiedSince() {
    return another.getIfModifiedSince();
  }

  @Override
  public void setIfModifiedSince(long ifmodifiedsince) {
    another.setIfModifiedSince(ifmodifiedsince);
  }

  @Override
  public boolean getDefaultUseCaches() {
    return another.getDefaultUseCaches();
  }

  @Override
  public void setDefaultUseCaches(boolean defaultusecaches) {
    another.setDefaultUseCaches(defaultusecaches);
  }

  @Override
  public void setRequestProperty(String key, String value) {
    another.setRequestProperty(key, value);
  }

  @Override
  public void addRequestProperty(String key, String value) {
    another.setRequestProperty(key, value);
  }

  @Override
  public String getRequestProperty(String key) {
    return another.getRequestProperty(key);
  }

  @Override
  public Map<String, List<String>> getRequestProperties() {
    return another.getRequestProperties();
  }

  @Override
  public String getResponseMessage() throws IOException {
    return another.getResponseMessage();
  }

  @Override
  public String getRequestMethod() {
    return method;
  }

  @Override
  public void setRequestMethod(String method) throws ProtocolException {
    another.setRequestMethod(method);
  }

  @Override
  public int getResponseCode() throws IOException {
    return another.getResponseCode();
  }

  @Override
  public void setFixedLengthStreamingMode(int contentLength) {
    another.setFixedLengthStreamingMode(contentLength);
  }

  @Override
  public void setChunkedStreamingMode(int chunklen) {
    another.setChunkedStreamingMode(chunklen);
  }

  @Override
  public boolean getInstanceFollowRedirects() {
    return another.getInstanceFollowRedirects();
  }

  @Override
  public void setInstanceFollowRedirects(boolean followRedirects) {
    another.setInstanceFollowRedirects(followRedirects);
  }

}
