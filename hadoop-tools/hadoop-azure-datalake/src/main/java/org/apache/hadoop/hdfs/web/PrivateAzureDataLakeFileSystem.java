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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.metrics.AdlFileSystemInstrumentation;
import org.apache.hadoop.hdfs.web.metrics.AdlFileSystemMetricsSystem;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2ConnectionConfigurator;
import org.apache.hadoop.hdfs.web.resources.ADLFlush;
import org.apache.hadoop.hdfs.web.resources.ADLGetOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLPostOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLPutOpParam;
import org.apache.hadoop.hdfs.web.resources.ADLVersionInfo;
import org.apache.hadoop.hdfs.web.resources.AppendADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.CreateADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.CreateFlagParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LeaseParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.ReadADLNoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extended @see SWebHdfsFileSystem API. This class contains Azure data lake
 * specific stability, Reliability and performance improvement.
 * <p>
 * Motivation behind PrivateAzureDataLakeFileSystem
 */
public class PrivateAzureDataLakeFileSystem extends SWebHdfsFileSystem {

  public static final String SCHEME = "adl";
  /**
   * Process wide thread pool for data lake file system.
   * Threads need to be daemon so that they dont prevent the process from
   * exiting
   */
  private static final ExecutorService EXECUTOR = Executors
      .newCachedThreadPool(new ThreadFactory() {
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          return t;
        }
      });
  private static String hostName = null;
  private static AtomicInteger metricsSourceNameCounter = new AtomicInteger();
  // Feature configuration
  // Publicly Exposed
  private boolean featureCacheFileStatus = true;
  private int featureCacheFileStatusDuration = ADLConfKeys
      .ADL_DEFAULT_CACHE_CURATION_TIME; // In Seconds
  private boolean featureGetBlockLocationLocallyBundled = true;
  private boolean featureConcurrentReadWithReadAhead = true;
  private boolean featureRedirectOff = true;
  // Internal Configuration
  private boolean featureFlushWhenEOF = true;
  private boolean featureOffsetSpecification = true;
  private String metricsSourceName;
  private FileStatusCacheManager fileStatusCacheManager = null;
  private boolean overrideOwner = false;
  private int maxConcurrentConnection;
  private int maxBufferSize;
  private String objectGuid = "";
  private AdlFileSystemInstrumentation instrumentation;
  private String userName;

  /**
   * Constructor.
   */
  public PrivateAzureDataLakeFileSystem() {
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      userName = "hadoop";
    }
    objectGuid = java.util.UUID.randomUUID().toString();
    fileStatusCacheManager = FileStatusCacheManager.getInstance();
  }

  /**
   * Extract computer name.
   *
   * @return name of the machine.
   */
  public static String getMachineName() {
    if (hostName != null) {
      return hostName;
    }

    try {
      InetAddress addr;
      addr = InetAddress.getLocalHost();
      hostName = addr.getHostName();
      return hostName;
    } catch (UnknownHostException ex) {
    }
    return "NA";
  }

  /**
   * Creates a new metrics source name that's unique within this process.
   */
  static String newMetricsSourceName() {
    int number = metricsSourceNameCounter.incrementAndGet();
    final String baseName = "AdlFileSystemMetrics";
    if (number == 1) { // No need for a suffix for the first one
      return baseName;
    } else {
      return baseName + number;
    }
  }

  protected boolean isFeatureCacheFileStatus() {
    return featureCacheFileStatus;
  }

  protected int getFeatureCacheFileStatusDuration() {
    return featureCacheFileStatusDuration;
  }

  protected boolean isFeatureGetBlockLocationLocallyBundled() {
    return featureGetBlockLocationLocallyBundled;
  }

  protected boolean isFeatureConcurrentReadWithReadAhead() {
    return featureConcurrentReadWithReadAhead;
  }

  protected boolean isFeatureRedirectOff() {
    return featureRedirectOff;
  }

  protected boolean isOverrideOwnerFeatureOn() {
    return overrideOwner;
  }

  protected int getMaxBufferSize() {
    return maxBufferSize;
  }

  protected int getMaxConcurrentConnection() {
    return maxConcurrentConnection;
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf)
      throws IOException {
    super.initialize(uri, conf);
    overrideOwner = getConf()
        .getBoolean(ADLConfKeys.ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER,
            ADLConfKeys.ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT);

    featureRedirectOff = getConf()
        .getBoolean(ADLConfKeys.ADL_FEATURE_REDIRECT_OFF,
            ADLConfKeys.ADL_FEATURE_REDIRECT_OFF_DEFAULT);

    featureCacheFileStatus = getConf()
        .getBoolean(ADLConfKeys.ADL_FEATURE_CACHE_FILE_STATUS,
            ADLConfKeys.ADL_FEATURE_CACHE_FILE_STATUS_DEFAULT);

    featureCacheFileStatusDuration = getConf().
        getInt(ADLConfKeys.
            ADL_FEATURE_CACHE_FILE_STATUS_DURATION, ADLConfKeys.
            ADL_FEATURE_CACHE_FILE_STATUS_DURATION_DEFAULT);

    featureGetBlockLocationLocallyBundled = getConf()
        .getBoolean(ADLConfKeys.ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED,
            ADLConfKeys.ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED_DEFAULT);

    featureConcurrentReadWithReadAhead = getConf().
        getBoolean(ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD,
            ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_DEFAULT);

    maxBufferSize = getConf().getInt(
        ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE,
        ADLConfKeys
            .ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE_DEFAULT);

    maxConcurrentConnection = getConf().getInt(
        ADLConfKeys.ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN,
        ADLConfKeys
            .ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN_DEFAULT);

    OAuth2ConnectionConfigurator connectionConfigurator = new
        OAuth2ConnectionConfigurator(
        conf);
    super.connectionFactory = new PrivateDebugUrlConnectionFactory(
        connectionConfigurator, objectGuid, conf);
    instrumentation = new AdlFileSystemInstrumentation(conf);
    AdlFileSystemMetricsSystem.fileSystemStarted();
    ADLLogger.log("Metrics system started successfully");

    metricsSourceName = newMetricsSourceName();
    String sourceDesc = "ADL File System metrics";
    AdlFileSystemMetricsSystem
        .registerSource(metricsSourceName, sourceDesc, instrumentation);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * Need to override default getHomeDirectory implementation due to
   * HDFS-8542 causing MR jobs to fail in initial
   * phase. Constructing home directory locally is fine as long as hadoop
   * local user name and ADL user name relation
   * ship is not agreed upon.
   *
   * @return Hadoop user home directory.
   */
  @Override
  public final Path getHomeDirectory() {
    try {
      return makeQualified(new Path(
          "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    } catch (IOException e) {
    }

    return new Path("/user/" + userName);
  }

  /**
   * Azure data lake does not support user configuration for data replication
   * hence not leaving system to query on
   * azure data lake.
   *
   * Stub implementation
   *
   * @param p           Not honoured
   * @param replication Not honoured
   * @return True hard coded since ADL file system does not support
   * replication configuration
   * @throws IOException No exception would not thrown in this case however
   *                     aligning with parent api definition.
   */
  @Override
  public final boolean setReplication(final Path p, final short replication)
      throws IOException {
    return true;
  }

  /**
   * Invoked parent setTimes default implementation only.
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance
   *
   * @param p     File/Folder path
   * @param mtime Modification time
   * @param atime Access time
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public final void setTimes(final Path p, final long mtime, final long atime)
      throws IOException {
    if (featureCacheFileStatus) {
      String filePath = p.isAbsoluteAndSchemeAuthorityNull() ?
          getUri() + p.toString() :
          p.toString();
      fileStatusCacheManager.remove(new Path(filePath));
    }
    super.setTimes(p, mtime, atime);
  }

  /**
   * Invokes parent setPermission default implementation only.
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance
   *
   * @param p          File/Folder path
   * @param permission Instance FsPermission. Octal values
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public final void setPermission(final Path p, final FsPermission permission)
      throws IOException {
    if (featureCacheFileStatus) {
      String filePath = p.isAbsoluteAndSchemeAuthorityNull() ?
          getUri() + p.toString() :
          p.toString();
      fileStatusCacheManager.remove(new Path(filePath));
    }
    super.setPermission(p, permission);
  }

  /**
   * Avoid call to Azure data lake backend system. Look in the local cache if
   * FileStatus from the previous call has
   * already been cached.
   *
   * Cache lookup is default enable. and can be set using configuration.
   *
   * @param f File/Folder path
   * @return FileStatus instance containing metadata information of f
   * @throws IOException For any system error
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    FileStatus status = null;
    if (featureCacheFileStatus) {
      status = fileStatusCacheManager.get(makeQualified(f));
    }

    if (status == null) {
      status = super.getFileStatus(f);
    } else {
      ADLLogger.log("Cached Instance Found : " + status.getPath());
    }

    if (featureCacheFileStatus) {
      if (fileStatusCacheManager.get(makeQualified(f)) == null) {
        fileStatusCacheManager.put(status, featureCacheFileStatusDuration);
      }
    }

    if (overrideOwner) {
      FileStatus proxiedStatus = new FileStatus(status.getLen(),
          status.isDirectory(), status.getReplication(), status.getBlockSize(),
          status.getModificationTime(), status.getAccessTime(),
          status.getPermission(), userName, "hdfs", status.getPath());
      return proxiedStatus;
    } else {
      return status;
    }
  }

  /**
   * Invokes parent delete() default implementation only.
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance
   *
   * @param f         File/Folder path
   * @param recursive true if the contents within folder needs to be removed
   *                  as well
   * @return true if the delete operation is successful other false.
   * @throws IOException For any system exception
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    if (featureCacheFileStatus) {
      FileStatus fs = fileStatusCacheManager.get(makeQualified(f));
      if (fs != null && fs.isFile()) {
        fileStatusCacheManager.remove(makeQualified(f));
      } else {
        fileStatusCacheManager.clear();
      }
    }
    instrumentation.fileDeleted();
    return super.delete(f, recursive);
  }

  /**
   * Invokes parent rename default implementation only.
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance     *
   *
   * @param src Source path
   * @param dst Destination path
   * @return True if the rename operation is successful otherwise false
   * @throws IOException For any system error.
   */
  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    if (featureCacheFileStatus) {
      FileStatus fsSrc = fileStatusCacheManager.get(makeQualified(src));
      FileStatus fsDst = fileStatusCacheManager.get(makeQualified(dst));

      if ((fsSrc != null && !fsSrc.isFile()) || (fsDst != null && !fsDst
          .isFile())) {
        fileStatusCacheManager.clear();
      } else {
        fileStatusCacheManager.remove(makeQualified(src));
        fileStatusCacheManager.remove(makeQualified(dst));
      }
    }
    return super.rename(src, dst);
  }

  /**
   * Overloaded version of rename. Invokes parent rename implementation only.
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance
   *
   * @param src     Source path
   * @param dst     Desitnation path
   * @param options Defined in webhdfs specification
   * @throws IOException For system error
   */
  @Override
  public void rename(Path src, Path dst, Options.Rename... options)
      throws IOException {
    if (featureCacheFileStatus) {
      FileStatus fsSrc = fileStatusCacheManager.get(makeQualified(src));
      FileStatus fsDst = fileStatusCacheManager.get(makeQualified(dst));

      if ((fsSrc != null && !fsSrc.isFile()) || (fsDst != null && !fsDst
          .isFile())) {
        fileStatusCacheManager.clear();
      } else {
        fileStatusCacheManager.remove(makeQualified(src));
        fileStatusCacheManager.remove(makeQualified(dst));
      }
    }
    super.rename(src, dst, options);
  }

  /**
   * Invokes parent append default implementation
   *
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance.
   *
   * @param f Stream path
   * @return Output stream.
   * @throws IOException For system error
   */
  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    String filePath = makeQualified(f).toString();
    fileStatusCacheManager.remove(new Path(filePath));
    return super.append(f);
  }

  /**
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance.
   *
   * @param f          Existing file path
   * @param bufferSize Size of the buffer
   * @param progress   Progress indicator
   * @return FSDataOutputStream OutputStream on which application can push
   * stream of bytes
   * @throws IOException For any system exception
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    String filePath = makeQualified(f).toString();
    fileStatusCacheManager.remove(new Path(filePath));
    return super.append(f, bufferSize, progress);
  }

  /**
   * Removes cached FileStatus entry to maintain latest information on the
   * FileStatus instance.
   *
   * @param trg  Target file path
   * @param srcs List of sources to be concatinated. ADL concatinate in the
   *             same order passed as parameter.
   * @throws IOException For any system exception
   */
  @Override
  public void concat(final Path trg, final Path[] srcs) throws IOException {
    if (featureCacheFileStatus) {
      String filePath = trg.isAbsoluteAndSchemeAuthorityNull() ?
          getUri() + trg.toString() :
          trg.toString();
      fileStatusCacheManager.remove(new Path(filePath));
      for (int i = 0; i < srcs.length; ++i) {
        filePath = srcs[0].isAbsoluteAndSchemeAuthorityNull() ?
            getUri() + srcs[0].toString() :
            srcs[0].toString();
        fileStatusCacheManager.remove(new Path(filePath));
      }
    }
    super.concat(trg, srcs);
  }

  /**
   * Create call semantic is handled differently in case of ADL. Create
   * semantics is translated to Create/Append
   * semantics.
   * 1. No dedicated connection to server.
   * 2. Buffering is locally done, Once buffer is full or flush is invoked on
   * the by the caller. All the pending
   * data is pushed to ADL as APPEND operation code.
   * 3. On close - Additional call is send to server to close the stream, and
   * release lock from the stream.
   *
   * Necessity of Create/Append semantics is
   * 1. ADL backend server does not allow idle connection for longer duration
   * . In case of slow writer scenario,
   * observed connection timeout/Connection reset causing occasional job
   * failures.
   * 2. Performance boost to jobs which are slow writer, avoided network latency
   * 3. ADL equally better performing with multiple of 4MB chunk as append
   * calls.
   *
   * @param f           File path
   * @param permission  Access perfrmission for the newly created file
   * @param overwrite   Remove existing file and recreate new one if true
   *                    otherwise throw error if file exist
   * @param bufferSize  Buffer size, ADL backend does not honour
   * @param replication Replication count, ADL backen does not hounour
   * @param blockSize   Block size, ADL backend does not honour
   * @param progress    Progress indicator
   * @return FSDataOutputStream OutputStream on which application can push
   * stream of bytes
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    // Increment the counter
    instrumentation.fileCreated();

    if (featureCacheFileStatus) {
      fileStatusCacheManager.remove(makeQualified(f));
    }

    return new FSDataOutputStream(new BatchAppendOutputStream(f, bufferSize,
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite), new BufferSizeParam(bufferSize),
        new ReplicationParam(replication), new BlockSizeParam(blockSize),
        new ADLVersionInfo(getADLEnabledFeatureSet())), statistics) {
    };
  }

  @Override
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    // Increment the counter
    instrumentation.fileCreated();

    if (featureCacheFileStatus) {
      String filePath = makeQualified(f).toString();
      fileStatusCacheManager.remove(new Path(filePath));
    }

    String leaseId = java.util.UUID.randomUUID().toString();
    return new FSDataOutputStream(new BatchAppendOutputStream(f, bufferSize,
        new PermissionParam(applyUMask(permission)), new CreateFlagParam(flag),
        new CreateParentParam(false), new BufferSizeParam(bufferSize),
        new ReplicationParam(replication), new LeaseParam(leaseId),
        new BlockSizeParam(blockSize),
        new ADLVersionInfo(getADLEnabledFeatureSet())), statistics) {
    };
  }

  /**
   * Since defined as private in parent class, redefined to pass through
   * Create api implementation.
   *
   * @param permission
   * @return FsPermission list
   */
  private FsPermission applyUMask(FsPermission permission) {
    FsPermission fsPermission = permission;
    if (fsPermission == null) {
      fsPermission = FsPermission.getDefault();
    }
    return fsPermission.applyUMask(FsPermission.getUMask(getConf()));
  }

  /**
   * Open call semantic is handled differently in case of ADL. Instead of
   * network stream is returned to the user,
   * Overridden FsInputStream is returned.
   *
   * 1. No dedicated connection to server.
   * 2. Process level concurrent read ahead Buffering is done, This allows
   * data to be available for caller quickly.
   * 3. Number of byte to read ahead is configurable.
   *
   * Advantage of Process level concurrent read ahead Buffering semantics is
   * 1. ADL backend server does not allow idle connection for longer duration
   * . In case of slow reader scenario,
   * observed connection timeout/Connection reset causing occasional job
   * failures.
   * 2. Performance boost to jobs which are slow reader, avoided network latency
   * 3. Compressed format support like ORC, and large data files gains the
   * most out of this implementation.
   *
   * Read ahead feature is configurable.
   *
   * @param f          File path
   * @param buffersize Buffer size
   * @return FSDataInputStream InputStream on which application can read
   * stream of bytes
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public FSDataInputStream open(final Path f, final int buffersize)
      throws IOException {
    long statContructionTime = System.currentTimeMillis();
    statistics.incrementReadOps(1);

    ADLLogger.log("statistics report Time " + (System.currentTimeMillis()
        - statContructionTime));

    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    // use a runner so the open can recover from an invalid token
    FsPathConnectionRunner runner = null;

    if (featureConcurrentReadWithReadAhead) {
      long urlContructionTime = System.currentTimeMillis();
      URL url = this.toUrl(op, f, new BufferSizeParam(buffersize),
          new ReadADLNoRedirectParam(true),
          new ADLVersionInfo(getADLEnabledFeatureSet()));
      ADLLogger.log("URL Construction Time " + (System.currentTimeMillis()
          - urlContructionTime));

      long bbContructionTime = System.currentTimeMillis();
      BatchByteArrayInputStream bb = new BatchByteArrayInputStream(url, f,
          maxBufferSize, maxConcurrentConnection);
      ADLLogger.log("BatchByteArrayInputStream Construction Time " + (
          System.currentTimeMillis() - bbContructionTime));

      long finContructionTime = System.currentTimeMillis();
      FSDataInputStream fin = new FSDataInputStream(bb);
      ADLLogger.log(
          "FSDataInputStream Construction Time " + (System.currentTimeMillis()
              - finContructionTime));
      return fin;
    } else {
      if (featureRedirectOff) {
        long urlContructionTime = System.currentTimeMillis();
        runner = new FsPathConnectionRunner(ADLGetOpParam.Op.OPEN, f,
            new BufferSizeParam(buffersize), new ReadADLNoRedirectParam(true),
            new ADLVersionInfo(getADLEnabledFeatureSet()));
        ADLLogger.log("Runner Construction Time " + (System.currentTimeMillis()
            - urlContructionTime));
      } else {
        runner = new FsPathConnectionRunner(op, f,
            new BufferSizeParam(buffersize));
      }

      return new FSDataInputStream(
          new OffsetUrlInputStream(new UnresolvedUrlOpener(runner),
              new OffsetUrlOpener(null)));
    }
  }

  /**
   * On successful response from the server, @see FileStatusCacheManger is
   * updated with FileStatus objects.
   *
   * @param f File/Folder path
   * @return FileStatus array list
   * @throws IOException For system error
   */
  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    FileStatus[] fileStatuses = super.listStatus(f);
    for (int i = 0; i < fileStatuses.length; i++) {
      if (featureCacheFileStatus) {
        fileStatusCacheManager
            .put(fileStatuses[i], featureCacheFileStatusDuration);
      }

      if (overrideOwner) {
        fileStatuses[i] = new FileStatus(fileStatuses[i].getLen(),
            fileStatuses[i].isDirectory(), fileStatuses[i].getReplication(),
            fileStatuses[i].getBlockSize(),
            fileStatuses[i].getModificationTime(),
            fileStatuses[i].getAccessTime(), fileStatuses[i].getPermission(),
            userName, "hdfs", fileStatuses[i].getPath());
      }
    }
    return fileStatuses;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }

    if (featureGetBlockLocationLocallyBundled) {
      if ((offset < 0) || (length < 0)) {
        throw new IllegalArgumentException("Invalid start or len parameter");
      }

      if (status.getLen() < offset) {
        if (ADLLogger.isLogEnabled()) {
          ADLLogger.log("getFileBlockLocations : Returning 1 block");
        }
        return new BlockLocation[0];
      }

      final String[] name = {"localhost"};
      final String[] host = {"localhost"};
      long blockSize = ADLConfKeys.DEFAULT_EXTENT_SIZE;
      if (blockSize <= 0) {
        throw new IllegalArgumentException(
            "The block size for the given file is not a positive number: "
                + blockSize);
      }
      int numberOfLocations =
          (int) (length / blockSize) + ((length % blockSize == 0) ? 0 : 1);
      BlockLocation[] locations = new BlockLocation[numberOfLocations];
      for (int i = 0; i < locations.length; i++) {
        long currentOffset = offset + (i * blockSize);
        long currentLength = Math
            .min(blockSize, offset + length - currentOffset);
        locations[i] = new BlockLocation(name, host, currentOffset,
            currentLength);
      }

      if (ADLLogger.isLogEnabled()) {
        ADLLogger.log("getFileBlockLocations : Returning " + locations.length
            + " Blocks");
      }

      return locations;

    } else {
      return getFileBlockLocations(status.getPath(), offset, length);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path p, final long offset,
      final long length) throws IOException {
    statistics.incrementReadOps(1);

    if (featureGetBlockLocationLocallyBundled) {
      FileStatus fileStatus = getFileStatus(p);
      return getFileBlockLocations(fileStatus, offset, length);
    } else {
      return super.getFileBlockLocations(p, offset, length);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    AdlFileSystemMetricsSystem.unregisterSource(metricsSourceName);
    AdlFileSystemMetricsSystem.fileSystemClosed();
  }

  private String getADLEnabledFeatureSet() {
    // TODO : Implement current feature set enabed for the instance.
    // example cache file status, reah ahead ..
    return ADLConfKeys.LOG_VERSION;
  }

  enum StreamState {
    Initial,
    DataCachedInLocalBuffer,
    StreamEnd
  }

  class BatchAppendOutputStream extends OutputStream {
    private Path fsPath;
    private Param<?, ?>[] parameters;
    private byte[] data = null;
    private int offset = 0;
    private long length = 0;
    private boolean eof = false;
    private boolean hadError = false;
    private int bufferIndex = 0;
    private byte[][] dataBuffers = new byte[2][];
    private int bufSize = 0;
    private Future<Object> flushTask = null;

    public BatchAppendOutputStream(Path path, int bufferSize,
        Param<?, ?>... param) throws IOException {
      if (bufferSize < (ADLConfKeys.DEFAULT_BLOCK_SIZE)) {
        bufSize = ADLConfKeys.DEFAULT_BLOCK_SIZE;
      } else {
        bufSize = bufferSize;
      }

      this.fsPath = path;
      this.parameters = param;
      this.data = getBuffer();
      FSDataOutputStream createStream = null;
      try {
        if (featureRedirectOff) {
          CreateADLNoRedirectParam skipRedirect = new CreateADLNoRedirectParam(
              true);
          Param<?, ?>[] tmpParam = featureFlushWhenEOF ?
              new Param<?, ?>[param.length + 2] :
              new Param<?, ?>[param.length + 1];
          System.arraycopy(param, 0, tmpParam, 0, param.length);
          tmpParam[param.length] = skipRedirect;
          if (featureFlushWhenEOF) {
            tmpParam[param.length + 1] = new ADLFlush(false);
          }
          createStream = new FsPathOutputStreamRunner(ADLPutOpParam.Op.CREATE,
              fsPath, 1, tmpParam).run();
        } else {
          createStream = new FsPathOutputStreamRunner(PutOpParam.Op.CREATE,
              fsPath, 1, param).run();
        }
      } finally {
        if (createStream != null) {
          createStream.close();
        }
      }
    }

    @Override
    public final synchronized void write(int b) throws IOException {
      if (offset == (data.length)) {
        flush();
      }

      data[offset] = (byte) b;
      offset++;

      // Statistics will get incremented again as part of the batch updates,
      // decrement here to avoid double value
      if (statistics != null) {
        statistics.incrementBytesWritten(-1);
      }
    }

    @Override
    public final synchronized void write(byte[] buf, int off, int len)
        throws IOException {
      int bytesToWrite = len;
      int localOff = off;
      int localLen = len;
      if (localLen >= data.length) {
        // Flush data that is already in our internal buffer
        flush();

        // Keep committing data until we have less than our internal buffers
        // length left
        do {
          try {
            commit(buf, localOff, data.length, eof);
          } catch (IOException e) {
            hadError = true;
            throw e;
          }
          localOff += data.length;
          localLen -= data.length;
        } while (localLen >= data.length);
      }

      // At this point, we have less than data.length left to copy from users
      // buffer
      if (offset + localLen >= data.length) {
        // Users buffer has enough data left to fill our internal buffer
        int bytesToCopy = data.length - offset;
        System.arraycopy(buf, localOff, data, offset, bytesToCopy);
        offset += bytesToCopy;

        // Flush our internal buffer asynchronously
        flushAsync();
        localOff += bytesToCopy;
        localLen -= bytesToCopy;
      }

      if (localLen > 0) {
        // Simply copy the remainder from the users buffer into our internal
        // buffer
        System.arraycopy(buf, localOff, data, offset, localLen);
        offset += localLen;
      }

      // Statistics will get incremented again as part of the batch updates,
      // decrement here to avoid double value
      if (statistics != null) {
        statistics.incrementBytesWritten(-bytesToWrite);
      }
      instrumentation.rawBytesUploaded(bytesToWrite);
    }

    @Override
    public final synchronized void flush() throws IOException {
      waitForOutstandingFlush();
      if (offset > 0) {
        try {
          commit(data, 0, offset, eof);
        } catch (IOException e) {
          hadError = true;
          throw e;
        }
      }

      offset = 0;
    }

    @Override
    public final synchronized void close() throws IOException {
      //TODO : 2ns call should not cause any error and no network calls.
      if (featureRedirectOff) {
        eof = true;
      }

      boolean flushedSomething = false;
      if (hadError) {
        // No point proceeding further since the error has occurered and
        // stream would be required to upload again.
        return;
      } else {
        flushedSomething = offset > 0;
        flush();
      }

      if (featureRedirectOff) {
        // If we didn't flush anything from our internal buffer, we have to
        // call the service again
        // with an empty payload and flush=true in the url
        if (!flushedSomething) {
          commit(null, 0, ADLConfKeys.KB, true);
        }
      }

      ADLLogger.log(" Total bytes Written : " + (length) + " [" + fsPath + "]");
    }

    private void commit(byte[] buffer, int off, int len, boolean endOfFile)
        throws IOException {
      OutputStream out = null;
      try {
        if (featureRedirectOff) {
          AppendADLNoRedirectParam skipRedirect = new AppendADLNoRedirectParam(
              true);
          Param<?, ?>[] tmpParam = featureFlushWhenEOF ?
              new Param<?, ?>[parameters.length + 3] :
              new Param<?, ?>[parameters.length + 1];
          System.arraycopy(parameters, 0, tmpParam, 0, parameters.length);
          tmpParam[parameters.length] = skipRedirect;
          if (featureFlushWhenEOF) {
            tmpParam[parameters.length + 1] = new ADLFlush(endOfFile);
            tmpParam[parameters.length + 2] = new OffsetParam(length);
          }

          out = new FsPathOutputStreamRunner(ADLPostOpParam.Op.APPEND, fsPath,
              len, tmpParam).run();
        } else {
          out = new FsPathOutputStreamRunner(ADLPostOpParam.Op.APPEND, fsPath,
              len, parameters).run();
        }

        if (buffer != null) {
          fileStatusCacheManager.remove(fsPath);
          out.write(buffer, off, len);
          length += len;
        }
      } finally {
        if (out != null) {
          out.close();
        }
      }
    }

    private void flushAsync() throws IOException {
      if (offset > 0) {
        waitForOutstandingFlush();

        // Submit the new flush task to the executor
        flushTask = EXECUTOR.submit(new CommitTask(data, offset, eof));

        // Get a new internal buffer for the user to write
        data = getBuffer();
        offset = 0;
      }
    }

    private void waitForOutstandingFlush() throws IOException {
      if (flushTask != null) {
        try {
          flushTask.get();
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        } catch (ExecutionException ex) {
          // Wrap the ExecutionException in an IOException for callers can
          // only handle IOException
          throw new IOException(ex);
        } finally {
          flushTask = null;
        }
      }
    }

    private byte[] getBuffer() {
      // Switch between the first and second buffer
      if (bufferIndex == 0) {
        if (dataBuffers[0] == null) {
          dataBuffers[0] = new byte[bufSize];
        }

        bufferIndex = 1;
        return dataBuffers[0];
      } else {
        if (dataBuffers[1] == null) {
          dataBuffers[1] = new byte[bufSize];
        }

        bufferIndex = 0;
        return dataBuffers[1];
      }
    }

    public class CommitTask implements Callable<Object> {
      private byte[] buff;
      private int len;
      private boolean eof;

      public CommitTask(byte[] buffer, int size, boolean isEnd) {
        buff = buffer;
        len = size;
        eof = isEnd;
      }

      public final Object call() throws IOException {
        commit(buff, 0, len, eof);
        return null;
      }
    }
  }

  @SuppressWarnings("checkstyle:javadocmethod")
  /**
   * Read data from backend in chunks instead of persistent connection. This
   * is to avoid slow reader causing socket
   * timeout.
   */ protected class BatchByteArrayInputStream extends FSInputStream {

    private static final int SIZE4MB = 4 * 1024 * 1024;
    private final URL runner;
    private volatile byte[] data = null;
    private volatile long validDataHoldingSize = 0;
    private volatile int bufferOffset = 0;
    private volatile long currentFileOffset = 0;
    private volatile long nextFileOffset = 0;
    private long fileSize = 0;
    private String guid;
    private StreamState state = StreamState.Initial;
    private int maxBufferSize;
    private int maxConcurrentConnection;
    private Path fsPath;
    private boolean streamIsClosed;
    private Future[] subtasks = null;

    BatchByteArrayInputStream(URL url, Path p, int bufferSize,
        int concurrentConnection) throws IOException {
      this.runner = url;
      fsPath = p;
      FileStatus fStatus = getFileStatus(fsPath);
      if (!fStatus.isFile()) {
        throw new IOException("Cannot open the directory " + p + " for " +
            "reading");
      }
      fileSize = fStatus.getLen();
      guid = getMachineName() + System.currentTimeMillis();
      this.maxBufferSize = bufferSize;
      this.maxConcurrentConnection = concurrentConnection;
      this.streamIsClosed = false;
    }

    @Override
    public final int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      long oldPos = this.getPos();

      int nread1;
      try {
        this.seek(position);
        nread1 = this.read(buffer, offset, length);
      } finally {
        this.seek(oldPos);
      }

      return nread1;
    }

    @Override
    public final int read() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      int status = doBufferAvailabilityCheck();
      if (status == -1) {
        return status;
      }
      int ch = data[bufferOffset++] & (0xff);
      if (statistics != null) {
        statistics.incrementBytesRead(1);
      }
      return ch;
    }

    @Override
    public final void readFully(long position, byte[] buffer, int offset,
        int length) throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      long startTime = System.currentTimeMillis();
      super.readFully(position, buffer, offset, length);
      ADLLogger.log("ReadFully1 Time Taken : " + (System.currentTimeMillis()
          - startTime));
      if (statistics != null) {
        statistics.incrementBytesRead(length);
      }
      instrumentation.rawBytesDownloaded(length);
    }

    @Override
    public final int read(byte[] b, int off, int len) throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      int status = doBufferAvailabilityCheck();
      if (status == -1) {
        return status;
      }

      long startTime = System.currentTimeMillis();
      int byteRead = 0;
      long availableBytes = validDataHoldingSize - off;
      long requestedBytes = bufferOffset + len - off;
      if (requestedBytes <= availableBytes) {
        if (b == null) {
          throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
          throw new IndexOutOfBoundsException();
        } else if (len == 0) {
          return 0;
        }

        ADLLogger.log("AC - [BufferOffset : " + bufferOffset + " " +
            "CurrentFileSite : " + currentFileOffset + "] Offset : " + off
            + " Length : " + len + " Read Time Taken : " + (
            System.currentTimeMillis() - startTime));
        try {
          System.arraycopy(data, bufferOffset, b, off, len);
        } catch (ArrayIndexOutOfBoundsException e) {
          ADLLogger.log("ArrayIndexOutOfBoundsException AC - [BufferOffset : " +
              "" + bufferOffset + " CurrentFileSite : " + currentFileOffset
              + "] Offset : " + off + " Length : " + len + " Read Time Taken : "
              + (System.currentTimeMillis() - startTime));
          throw e;
        }
        bufferOffset += len;
        byteRead = len;
      } else {
        ADLLogger.log("HC - [BufferOffset : " + bufferOffset + " " +
            "CurrentFileSite : " + currentFileOffset + "] Offset : " + off
            + " Length : " + len + " Read Time Taken : " + (
            System.currentTimeMillis() - startTime));
        byteRead = super.read(b, off, len);
      }
      if (statistics != null) {
        statistics.incrementBytesRead(byteRead);
      }
      instrumentation.rawBytesDownloaded(byteRead);
      return byteRead;
    }

    private int doBufferAvailabilityCheck() throws IOException {
      if (state == StreamState.Initial) {
        ADLLogger.log("Initial Fill");
        validDataHoldingSize = fill(nextFileOffset);
      }

      long dataReloadSize = 0;
      switch ((int) validDataHoldingSize) {
      case -1:
        state = StreamState.StreamEnd;
        return -1;
      case 0:
        dataReloadSize = fill(nextFileOffset);
        if (dataReloadSize <= 0) {
          state = StreamState.StreamEnd;
          return (int) dataReloadSize;
        } else {
          validDataHoldingSize = dataReloadSize;
        }
        break;
      default:
        break;
      }

      if (bufferOffset >= validDataHoldingSize) {
        dataReloadSize = fill(nextFileOffset);
      }

      if (bufferOffset >= ((dataReloadSize == 0) ?
          validDataHoldingSize :
          dataReloadSize)) {
        state = StreamState.StreamEnd;
        return -1;
      }

      validDataHoldingSize = ((dataReloadSize == 0) ?
          validDataHoldingSize :
          dataReloadSize);
      state = StreamState.DataCachedInLocalBuffer;
      return 0;
    }

    public final long fill(final long off) throws IOException {
      ADLLogger.log("Fill from " + off);
      long startTime = System.currentTimeMillis();
      if (state == StreamState.StreamEnd) {
        return -1;
      }

      if (fileSize <= off) {
        state = StreamState.StreamEnd;
        return -1;
      }
      int len = maxBufferSize;
      long fileOffset = 0;
      boolean isEntireFileCached = true;
      if ((fileSize < maxBufferSize)) {
        len = (int) fileSize;
        currentFileOffset = 0;
        nextFileOffset = 0;
      } else {
        if (len > (fileSize - off)) {
          len = (int) (fileSize - off);
        }

        if (BufferManager.getInstance()
            .hasValidDataForOffset(fsPath.toString(), off)) {
          len = (int) (
              BufferManager.getInstance().getBufferOffset() + BufferManager
                  .getInstance().getBufferSize() - (int) off);
        }

        if (len <= 0) {
          len = maxBufferSize;
        }
        fileOffset = off;
        isEntireFileCached = false;
      }

      data = null;
      BufferManager bm = BufferManager.getInstance();
      data = bm.getEmpty(len);
      boolean fetchDataOverNetwork = false;
      if (bm.hasData(fsPath.toString(), fileOffset, len)) {
        try {
          bm.get(data, fileOffset);
          validDataHoldingSize = data.length;
          currentFileOffset = fileOffset;
        } catch (ArrayIndexOutOfBoundsException e) {
          fetchDataOverNetwork = true;
        }
      } else {
        fetchDataOverNetwork = true;
      }

      if (fetchDataOverNetwork) {
        int splitSize = getSplitSize(len);
        try {
          validDataHoldingSize = fillDataConcurrently(data, len, fileOffset,
              splitSize);
        } catch (InterruptedException e) {
          throw new IOException(e.getMessage());
        }
        bm.add(data, fsPath.toString(), fileOffset);
        currentFileOffset = nextFileOffset;
      }

      nextFileOffset += validDataHoldingSize;
      state = StreamState.DataCachedInLocalBuffer;
      bufferOffset = isEntireFileCached ? (int) off : 0;
      ADLLogger.log("Buffer Refill Time Taken : " + (System.currentTimeMillis()
          - startTime));
      return validDataHoldingSize;
    }

    int getSplitSize(int size) {
      if (size <= SIZE4MB) {
        return 1;
      }

      // Not practical
      if (size > maxBufferSize) {
        size = maxBufferSize;
      }

      int equalBufferSplit = size / SIZE4MB;
      int splitSize = Math.min(equalBufferSplit, maxConcurrentConnection);
      return splitSize;
    }

    @Override
    public final void seek(long pos) throws IOException {
      if (pos == -1) {
        throw new IOException("Bad offset, cannot seek to " + pos);
      }

      BufferManager bm = BufferManager.getInstance();
      if (bm.hasValidDataForOffset(fsPath.toString(), pos)) {
        state = StreamState.DataCachedInLocalBuffer;
      } else if (pos >= 0) {
        state = StreamState.Initial;
      }

      long availableBytes = (currentFileOffset + validDataHoldingSize);
      ADLLogger.log("SEEK  : " + pos + " Available " + currentFileOffset + " " +
          "Count " + availableBytes);

      // Check if this position falls under buffered data
      if (pos < currentFileOffset || availableBytes <= 0) {
        validDataHoldingSize = 0;
        currentFileOffset = pos;
        nextFileOffset = pos;
        bufferOffset = 0;
        return;
      }

      if (pos < availableBytes && pos >= currentFileOffset) {
        state = StreamState.DataCachedInLocalBuffer;
        bufferOffset = (int) (pos - currentFileOffset);
      } else {
        validDataHoldingSize = 0;
        currentFileOffset = pos;
        nextFileOffset = pos;
        bufferOffset = 0;
      }
    }

    @Override
    public final long getPos() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      return bufferOffset + currentFileOffset;
    }

    @Override
    public final int available() throws IOException {
      if (streamIsClosed) {
        throw new IOException("Stream already closed");
      }
      return Integer.MAX_VALUE;
    }

    @Override
    public final boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @SuppressWarnings("unchecked")
    public final int fillDataConcurrently(byte[] byteArray, int length,
        long globalOffset, int splitSize)
        throws IOException, InterruptedException {
      ADLLogger.log("Fill up Data from " + globalOffset + " len : " + length);
      ExecutorService executor = Executors.newFixedThreadPool(splitSize);
      subtasks = new Future[splitSize];
      for (int i = 0; i < splitSize; i++) {
        int offset = i * (length / splitSize);
        int splitLength = (splitSize == (i + 1)) ?
            (length / splitSize) + (length % splitSize) :
            (length / splitSize);
        subtasks[i] = executor.submit(
            new BackgroundReadThread(byteArray, offset, splitLength,
                globalOffset + offset));
      }

      executor.shutdown();
      // wait until all tasks are finished
      try {
        executor.awaitTermination(ADLConfKeys.DEFAULT_TIMEOUT_IN_SECONDS,
            TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        ADLLogger.log("Interupted : " + e.getMessage());
        throw e;
      }

      int totalBytePainted = 0;
      for (int i = 0; i < splitSize; ++i) {
        try {
          totalBytePainted += (Integer) subtasks[i].get();
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }

      if (totalBytePainted != length) {
        throw new IOException("Expected " + length + " bytes, Got " +
            totalBytePainted + " bytes");
      }

      return totalBytePainted;
    }

    @Override
    public final void close() throws IOException {
      BufferManager.getInstance().clear();

      //need to cleanup the above code the stream and connection close doesnt
      // happen here
      //flag set to mark close happened, cannot use the stream once closed
      streamIsClosed = true;
    }

    private int fillUpData(byte[] buffer, int offset, int length,
        long globalOffset) throws IOException {
      int totalBytesRead = 0;
      final URL offsetUrl = new URL(
          runner + "&" + new OffsetParam(String.valueOf(globalOffset)) + "&"
              + new LengthParam(String.valueOf(length)) + "&openid=" + guid);
      HttpURLConnection conn = new URLRunner(GetOpParam.Op.OPEN, offsetUrl,
          true).run();
      InputStream in = conn.getInputStream();
      try {
        int bytesRead = 0;
        while ((bytesRead = in.read(buffer, (int) offset + totalBytesRead,
            (int) (length - totalBytesRead))) > 0) {
          totalBytesRead += bytesRead;
        }

        // InputStream must be fully consumed to enable http keep-alive
        if (bytesRead == 0) {
          // Looking for EOF marker byte needs to be read.
          if (in.read() != -1) {
            throw new SocketException(
                "Server returned more than requested " + "data.");
          }
        }
      } finally {
        in.close();
        conn.disconnect();
      }

      return totalBytesRead;
    }

    private class BackgroundReadThread implements Callable {

      private final byte[] data;
      private int offset;
      private int length;
      private long globalOffset;

      BackgroundReadThread(byte[] buffer, int off, int size, long position) {
        this.data = buffer;
        this.offset = off;
        this.length = size;
        this.globalOffset = position;
      }

      public Object call() throws IOException {
        return fillUpData(data, offset, length, globalOffset);
      }
    }
  }
}