/**
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
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NON_LOCAL_LAZY_PERSIST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BP_READY_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.security.SaslPropertiesResolver;

import java.util.concurrent.TimeUnit;

/**
 * Simple class encapsulating all of the configuration that the DataNode
 * loads at startup time.
 */
@InterfaceAudience.Private
public class DNConf {
  final Configuration conf;
  final int socketTimeout;
  final int socketWriteTimeout;
  final int socketKeepaliveTimeout;
  private final int transferSocketSendBufferSize;
  private final int transferSocketRecvBufferSize;
  private final boolean tcpNoDelay;

  final boolean transferToAllowed;
  final boolean dropCacheBehindWrites;
  final boolean syncBehindWrites;
  final boolean syncBehindWritesInBackground;
  final boolean dropCacheBehindReads;
  final boolean syncOnClose;
  final boolean encryptDataTransfer;
  final boolean connectToDnViaHostname;

  final long readaheadLength;
  final long heartBeatInterval;
  private final long lifelineIntervalMs;
  final long blockReportInterval;
  final long blockReportSplitThreshold;
  final long ibrInterval;
  final long initialBlockReportDelayMs;
  final long cacheReportInterval;
  final long dfsclientSlowIoWarningThresholdMs;
  final long datanodeSlowIoWarningThresholdMs;
  final int writePacketSize;
  
  final String minimumNameNodeVersion;
  final String encryptionAlgorithm;
  final SaslPropertiesResolver saslPropsResolver;
  final TrustedChannelResolver trustedChannelResolver;
  private final boolean ignoreSecurePortsForTesting;
  
  final long xceiverStopTimeout;
  final long restartReplicaExpiry;

  final long maxLockedMemory;

  private final long bpReadyTimeout;

  // Allow LAZY_PERSIST writes from non-local clients?
  private final boolean allowNonLocalLazyPersist;

  private final int volFailuresTolerated;
  private final int volsConfigured;
  private final int maxDataLength;

  public DNConf(Configuration conf) {
    this.conf = conf;
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsConstants.READ_TIMEOUT);
    socketWriteTimeout = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsConstants.WRITE_TIMEOUT);
    socketKeepaliveTimeout = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT);
    this.transferSocketSendBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_SEND_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_SEND_BUFFER_SIZE_DEFAULT);
    this.transferSocketRecvBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
    this.tcpNoDelay = conf.getBoolean(
        DFSConfigKeys.DFS_DATA_TRANSFER_SERVER_TCPNODELAY,
        DFSConfigKeys.DFS_DATA_TRANSFER_SERVER_TCPNODELAY_DEFAULT);

    /* Based on results on different platforms, we might need set the default
     * to false on some of them. */
    transferToAllowed = conf.getBoolean(
        DFS_DATANODE_TRANSFERTO_ALLOWED_KEY,
        DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT);

    writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
        DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    
    readaheadLength = conf.getLong(
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
    maxDataLength = conf.getInt(DFSConfigKeys.IPC_MAXIMUM_DATA_LENGTH,
        DFSConfigKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    dropCacheBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT);
    syncBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT);
    syncBehindWritesInBackground = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_KEY,
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_DEFAULT);
    dropCacheBehindReads = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT);
    connectToDnViaHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.blockReportInterval = conf.getLong(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    this.ibrInterval = conf.getLong(
        DFSConfigKeys.DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_KEY,
        DFSConfigKeys.DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_DEFAULT);
    this.blockReportSplitThreshold = conf.getLong(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY,
                                            DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT);
    this.cacheReportInterval = conf.getLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
        DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);

    this.dfsclientSlowIoWarningThresholdMs = conf.getLong(
        HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    this.datanodeSlowIoWarningThresholdMs = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

    long initBRDelay = conf.getTimeDuration(
        DFS_BLOCKREPORT_INITIAL_DELAY_KEY,
        DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT, TimeUnit.SECONDS) * 1000L;
    if (initBRDelay >= blockReportInterval) {
      initBRDelay = 0;
      DataNode.LOG.info("dfs.blockreport.initialDelay is "
          + "greater than or equal to" + "dfs.blockreport.intervalMsec."
          + " Setting initial delay to 0 msec:");
    }
    initialBlockReportDelayMs = initBRDelay;
    
    heartBeatInterval = conf.getTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS) * 1000L;
    long confLifelineIntervalMs =
        conf.getLong(DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY,
        3 * conf.getTimeDuration(DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS)) * 1000L;
    if (confLifelineIntervalMs <= heartBeatInterval) {
      confLifelineIntervalMs = 3 * heartBeatInterval;
      DataNode.LOG.warn(
          String.format("%s must be set to a value greater than %s.  " +
              "Resetting value to 3 * %s, which is %d milliseconds.",
              DFS_DATANODE_LIFELINE_INTERVAL_SECONDS_KEY,
              DFS_HEARTBEAT_INTERVAL_KEY, DFS_HEARTBEAT_INTERVAL_KEY,
              confLifelineIntervalMs));
    }
    lifelineIntervalMs = confLifelineIntervalMs;
    
    // do we need to sync block file contents to disk when blockfile is closed?
    this.syncOnClose = conf.getBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, 
        DFS_DATANODE_SYNCONCLOSE_DEFAULT);

    this.minimumNameNodeVersion = conf.get(DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY,
        DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT);
    
    this.encryptDataTransfer = conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
        DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    this.encryptionAlgorithm = conf.get(DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    this.trustedChannelResolver = TrustedChannelResolver.getInstance(conf);
    this.saslPropsResolver = DataTransferSaslUtil.getSaslPropertiesResolver(
      conf);
    this.ignoreSecurePortsForTesting = conf.getBoolean(
        IGNORE_SECURE_PORTS_FOR_TESTING_KEY,
        IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT);
    
    this.xceiverStopTimeout = conf.getLong(
        DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY,
        DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);

    this.maxLockedMemory = conf.getLong(
        DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT);

    this.restartReplicaExpiry = conf.getLong(
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY,
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT) * 1000L;

    this.allowNonLocalLazyPersist = conf.getBoolean(
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST,
        DFS_DATANODE_NON_LOCAL_LAZY_PERSIST_DEFAULT);

    this.bpReadyTimeout = conf.getTimeDuration(
        DFS_DATANODE_BP_READY_TIMEOUT_KEY,
        DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT, TimeUnit.SECONDS);

    this.volFailuresTolerated =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
            DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);
    String[] dataDirs =
        conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    this.volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
  }

  // We get minimumNameNodeVersion via a method so it can be mocked out in tests.
  String getMinimumNameNodeVersion() {
    return this.minimumNameNodeVersion;
  }
  
  /**
   * Returns the configuration.
   * 
   * @return Configuration the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Returns true if encryption enabled for DataTransferProtocol.
   *
   * @return boolean true if encryption enabled for DataTransferProtocol
   */
  public boolean getEncryptDataTransfer() {
    return encryptDataTransfer;
  }

  /**
   * Returns encryption algorithm configured for DataTransferProtocol, or null
   * if not configured.
   *
   * @return encryption algorithm configured for DataTransferProtocol
   */
  public String getEncryptionAlgorithm() {
    return encryptionAlgorithm;
  }

  public long getXceiverStopTimeout() {
    return xceiverStopTimeout;
  }

  public long getMaxLockedMemory() {
    return maxLockedMemory;
  }

  /**
   * Returns true if connect to datanode via hostname
   * 
   * @return boolean true if connect to datanode via hostname
   */
  public boolean getConnectToDnViaHostname() {
    return connectToDnViaHostname;
  }

  /**
   * Returns socket timeout
   * 
   * @return int socket timeout
   */
  public int getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * Returns socket write timeout
   * 
   * @return int socket write timeout
   */
  public int getSocketWriteTimeout() {
    return socketWriteTimeout;
  }

  /**
   * Returns the SaslPropertiesResolver configured for use with
   * DataTransferProtocol, or null if not configured.
   *
   * @return SaslPropertiesResolver configured for use with DataTransferProtocol
   */
  public SaslPropertiesResolver getSaslPropsResolver() {
    return saslPropsResolver;
  }

  /**
   * Returns the TrustedChannelResolver configured for use with
   * DataTransferProtocol, or null if not configured.
   *
   * @return TrustedChannelResolver configured for use with DataTransferProtocol
   */
  public TrustedChannelResolver getTrustedChannelResolver() {
    return trustedChannelResolver;
  }

  /**
   * Returns true if configuration is set to skip checking for proper
   * port configuration in a secured cluster.  This is only intended for use in
   * dev testing.
   *
   * @return true if configured to skip checking secured port configuration
   */
  public boolean getIgnoreSecurePortsForTesting() {
    return ignoreSecurePortsForTesting;
  }

  public boolean getAllowNonLocalLazyPersist() {
    return allowNonLocalLazyPersist;
  }

  public int getTransferSocketRecvBufferSize() {
    return transferSocketRecvBufferSize;
  }

  public int getTransferSocketSendBufferSize() {
    return transferSocketSendBufferSize;
  }

  public boolean getDataTransferServerTcpNoDelay() {
    return tcpNoDelay;
  }

  public long getBpReadyTimeout() {
    return bpReadyTimeout;
  }

  /**
   * Returns the interval in milliseconds between sending lifeline messages.
   *
   * @return interval in milliseconds between sending lifeline messages
   */
  public long getLifelineIntervalMs() {
    return lifelineIntervalMs;
  }

  public int getVolFailuresTolerated() {
    return volFailuresTolerated;
  }

  public int getVolsConfigured() {
    return volsConfigured;
  }

  int getMaxDataLength() {
    return maxDataLength;
  }
}
