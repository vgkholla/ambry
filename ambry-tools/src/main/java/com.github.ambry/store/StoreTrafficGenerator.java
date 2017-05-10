/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatMetrics;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StoreTrafficGenerator {
  private static final Logger logger = LoggerFactory.getLogger(StoreTrafficGenerator.class);

  private final TrafficGeneratorConfig generatorConfig;
  private final ClusterMapConfig clusterMapConfig;
  private final StoreConfig storeConfig;
  private final byte[] blob;
  private final Bootstrap b = new Bootstrap();
  private final ChannelConnectListener channelConnectListener = new ChannelConnectListener();
  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
  private final StoreTrafficGeneratorMetrics metrics = new StoreTrafficGeneratorMetrics(metricRegistry);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicLong totalRequestCount = new AtomicLong(0);
  private final Queue<HttpMethod> methodSelectorQueue = new LinkedBlockingQueue<>();
  private final List<String> blobIds = Collections.synchronizedList(new ArrayList<String>());
  private final Map<String, Long> posted = new ConcurrentHashMap<>();
  private final Set<String> deleted = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Set<String> deleteCandidates = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Map<HttpMethod, Throttler> throttlers = new ConcurrentHashMap<>();

  private ClusterMap clusterMap;
  private StoreKeyFactory storeKeyFactory;
  private Log log;
  private PersistentIndex index;

  private EventLoopGroup group;
  private long generatorStartTime;
  private volatile boolean verificationMode = false;
  private volatile Queue<String> blobsToVerify;
  private volatile boolean isRunning = false;

  /**
   * Configuration for the {@link StoreTrafficGenerator}.
   */
  private static class TrafficGeneratorConfig {

    @Config("host")
    @Default("localhost")
    final String host;

    @Config("port")
    @Default("1174")
    final int port;

    @Config("post.blob.max.size")
    @Default("4 * 1024 * 1024")
    final int postBlobMaxSize;

    @Config("max.posts.per.sec")
    @Default("1")
    final int maxPostsPerSec;

    @Config("max.gets.per.sec")
    @Default("30")
    final int maxGetsPerSec;

    @Config("max.deletes.per.sec")
    @Default("3")
    final int maxDeletesPerSec;

    /**
     * The path of the directory where the the pre-compaction store files are.
     */
    @Config("store.dir")
    final String storeDirPath;

    /**
     * The path to the hardware layout file.
     */
    @Config("hardware.layout.file.path")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file.
     */
    @Config("partition.layout.file.path")
    final String partitionLayoutFilePath;

    /**
     * Loads the config.
     * @param verifiableProperties the {@link VerifiableProperties} to load the config from.
     */
    TrafficGeneratorConfig(VerifiableProperties verifiableProperties) {
      host = verifiableProperties.getString("host", "localhost");
      port = verifiableProperties.getIntInRange("port", 1174, 0, 65536);
      postBlobMaxSize = verifiableProperties.getIntInRange("post.blob.max.size", 4 * 1024 * 1024, 1, Integer.MAX_VALUE);
      maxPostsPerSec = verifiableProperties.getIntInRange("max.posts.per.sec", 1, 0, Integer.MAX_VALUE);
      maxGetsPerSec = verifiableProperties.getIntInRange("max.gets.per.sec", 30, 0, Integer.MAX_VALUE);
      maxDeletesPerSec = verifiableProperties.getIntInRange("max.deletes.per.sec", 3, 0, Integer.MAX_VALUE);
      storeDirPath = verifiableProperties.getString("store.dir");
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path");
    }
  }

  /**
   * Invokes the {@link StoreTrafficGenerator} with the command line arguments.
   * @param args command line arguments.
   */
  public static void main(String[] args) throws Exception {
    final File shutdownFile = new File("/tmp/trafficGenStatus");
    if (shutdownFile.exists() && !shutdownFile.delete()) {
      logger.error("Could not delete {}", shutdownFile);
    }
    VerifiableProperties verifiableProperties = StoreToolsUtil.getVerifiableProperties(args);
    final StoreTrafficGenerator generator = new StoreTrafficGenerator(new TrafficGeneratorConfig(verifiableProperties),
        new ClusterMapConfig(verifiableProperties), new StoreConfig(verifiableProperties));
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("Received shutdown signal. Starting verification ");
        generator.verifyPostsAndDeletes();
        logger.info("Requesting StoreTrafficGenerator shutdown");
        generator.shutdown();
        try {
          int status = generator.isSuccess() ? 0 : 1;
          File tmpFile = new File("/tmp/trafficGenStatus.tmp");
          FileOutputStream stream = new FileOutputStream(tmpFile);
          stream.write(Integer.toString(status).getBytes());
          stream.close();
          logger.debug("Renaming {} to {}", tmpFile, shutdownFile);
          if (!tmpFile.renameTo(shutdownFile)) {
            logger.error("Could not rename {} to {}", tmpFile, shutdownFile);
          }
        } catch (Exception e) {
          logger.error("Could not shutdown", e);
        }
      }
    });
    generator.start();
    generator.awaitShutdown();
  }

  /**
   * Creates an instance of {@link StoreTrafficGenerator}
   * @param generatorConfig the {@link TrafficGeneratorConfig} to use.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param storeConfig the {@link StoreConfig} to use.
   * @throws IOException
   */
  private StoreTrafficGenerator(TrafficGeneratorConfig generatorConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig) throws IOException, JSONException, StoreException {
    this.generatorConfig = generatorConfig;
    this.clusterMapConfig = clusterMapConfig;
    this.storeConfig = storeConfig;
    blob = new byte[generatorConfig.postBlobMaxSize];
    new Random().nextBytes(blob);
    logger.info("Instantiated StoreTrafficGenerator");
  }

  /**
   * Starts the StoreTrafficGenerator.
   * @throws Exception
   */
  private void start() throws Exception {
    logger.info("Starting StoreTrafficGenerator");
    reporter.start();
    clusterMap = new StaticClusterAgentsFactory(clusterMapConfig, generatorConfig.hardwareLayoutFilePath,
        generatorConfig.partitionLayoutFilePath).getClusterMap();
    StoreMetrics storeMetrics = new StoreMetrics("", metricRegistry);
    storeKeyFactory = new BlobIdFactory(clusterMap);
    log = new Log(generatorConfig.storeDirPath, Long.MAX_VALUE, -1, storeMetrics);
    index = new PersistentIndex(generatorConfig.storeDirPath, null, log, storeConfig, storeKeyFactory,
        new BlobStoreRecovery(), null, storeMetrics, SystemTime.getInstance(), UUID.randomUUID(), UUID.randomUUID());
    loadAllIds();
    throttlers.put(HttpMethod.POST, new Throttler(generatorConfig.maxPostsPerSec, 1, true, SystemTime.getInstance()));
    int getRatePerSec = Math.max(generatorConfig.maxGetsPerSec, 1);
    throttlers.put(HttpMethod.GET, new Throttler(getRatePerSec, 1, true, SystemTime.getInstance()));
    throttlers.put(HttpMethod.DELETE,
        new Throttler(generatorConfig.maxDeletesPerSec, 1, true, SystemTime.getInstance()));
    int concurrency = generatorConfig.maxPostsPerSec + generatorConfig.maxGetsPerSec + generatorConfig.maxDeletesPerSec;
    group = new NioEventLoopGroup(concurrency);
    b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpClientCodec()).addLast(new ChunkedWriteHandler()).addLast(new ResponseHandler());
      }
    });
    logger.info("Connecting to {}:{}", generatorConfig.host, generatorConfig.port);
    b.remoteAddress(generatorConfig.host, generatorConfig.port);
    generatorStartTime = System.currentTimeMillis();
    for (int i = 0; i < generatorConfig.maxPostsPerSec; i++) {
      methodSelectorQueue.offer(HttpMethod.POST);
      b.connect().addListener(channelConnectListener);
    }
    for (int i = 0; i < generatorConfig.maxGetsPerSec; i++) {
      methodSelectorQueue.offer(HttpMethod.GET);
      b.connect().addListener(channelConnectListener);
    }
    for (int i = 0; i < generatorConfig.maxDeletesPerSec; i++) {
      methodSelectorQueue.offer(HttpMethod.DELETE);
      b.connect().addListener(channelConnectListener);
    }
    isRunning = true;
    logger.info("Created {} channel(s)", concurrency);
    logger.info("StoreTrafficGenerator started");
  }

  private void verifyPostsAndDeletes() {
    try {
      blobsToVerify = new LinkedBlockingQueue<>(posted.keySet());
      blobsToVerify.addAll(deleted);
      int left = blobsToVerify.size();
      verificationMode = true;
      while (left > 0) {
        logger.info("{} blobs left to verify", left);
        Thread.sleep(1000);
        left = blobsToVerify.size();
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Shuts down the StoreTrafficGenerator.
   */
  private void shutdown() {
    logger.info("Shutting down StoreTrafficGenerator");
    isRunning = false;
    group.shutdownGracefully();
    try {
      if (!group.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.error("Netty worker did not shutdown within timeout");
      } else {
        logger.info("StoreTrafficGenerator shutdown complete");
      }
      for (Throttler throttler : throttlers.values()) {
        throttler.close();
      }
      index.close();
      log.close();
      clusterMap.close();
    } catch (Exception e) {
      logger.error("StoreTrafficGenerator shutdown interrupted", e);
    } finally {
      printMetrics();
      reporter.stop();
      shutdownLatch.countDown();
    }
  }

  private void loadAllIds() throws IOException {
    for (IndexSegment indexSegment : index.getIndexSegments().values()) {
      List<IndexEntry> indexEntries = new ArrayList<>();
      indexSegment.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), indexEntries,
          new AtomicLong(0));
      for (IndexEntry entry : indexEntries) {
        StoreKey key = entry.getKey();
        IndexValue value = entry.getValue();
        if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
          // add ID only if PUT is in the same segment (blob IDs of orphaned deletes not added if origOffset == -1)
          long originalMessageOffset = value.getOriginalMessageOffset();
          if (originalMessageOffset != -1 && originalMessageOffset >= indexSegment.getStartOffset().getOffset()) {
            blobIds.add(key.getID());
          }
        } else {
          blobIds.add(key.getID());
        }
      }
    }
  }

  private void printMetrics() {
    long totalRunTimeInMs = System.currentTimeMillis() - generatorStartTime;
    logger.info("Executed for approximately {} s and sent {} requests ({} requests/sec)",
        (float) totalRunTimeInMs / (float) Time.MsPerSec, totalRequestCount.get(),
        (float) totalRequestCount.get() * (float) Time.MsPerSec / (float) totalRunTimeInMs);
    Snapshot rttStatsSnapshot = metrics.requestRoundTripTimeInMs.getSnapshot();
    logger.info("RTT stats: Min - {} ms, Mean - {} ms, Max - {} ms", rttStatsSnapshot.getMin(),
        rttStatsSnapshot.getMean(), rttStatsSnapshot.getMax());
    logger.info("RTT stats: 95th percentile - {} ms, 99th percentile - {} ms, 999th percentile - {} ms",
        rttStatsSnapshot.get95thPercentile(), rttStatsSnapshot.get99thPercentile(),
        rttStatsSnapshot.get999thPercentile());
    logger.info("POST errors: {}, GET errors: {}, DELETE errors: {}", metrics.postError.getCount(),
        metrics.getError.getCount(), metrics.deleteError.getCount());
    logger.info("Connect errors: {}, Request/Response errors: {}, Unexpected disconnections: {}",
        metrics.connectError.getCount(), metrics.requestResponseError.getCount(),
        metrics.unexpectedDisconnectionError.getCount());
  }

  private boolean isSuccess() {
    return (metrics.postError.getCount() + metrics.getError.getCount() + metrics.deleteError.getCount()
        + metrics.connectError.getCount() + metrics.requestResponseError.getCount()
        + metrics.unexpectedDisconnectionError.getCount()) == 0;
  }

  /**
   * Blocking function to wait on the StoreTrafficGenerator shutting down.
   * @throws InterruptedException
   */
  private void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  /**
   * Custom handler that sends out the request and receives and processes the response.
   */
  private class ResponseHandler extends SimpleChannelInboundHandler<HttpObject> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Random random = new Random();

    private HttpMethod httpMethod;
    private HttpRequest request;
    private HttpResponse response;
    private ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
    private long requestStartTime;
    private int chunksReceived;
    private long sizeReceived;
    private long lastChunkReceiveTime;
    private long requestId = 0;

    // applicable to POST only
    private ChunkedInput<HttpContent> chunkedInput;

    // applicable to GET only
    private boolean couldReceiveOK;

    // applicable to GET and DELETE.
    private String blobId;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      metrics.channelCreationRate.mark();
      httpMethod = verificationMode ? HttpMethod.GET : methodSelectorQueue.poll();
      logger.info("Channel {} active and will run method {}", ctx.channel(), httpMethod);
      sendRequest(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject in) throws Exception {
      long currentChunkReceiveTime = System.currentTimeMillis();
      boolean recognized = false;
      if (in instanceof HttpResponse) {
        recognized = true;
        long responseReceiveStart = currentChunkReceiveTime - requestStartTime;
        metrics.timeToFirstResponseChunkInMs.update(responseReceiveStart);
        logger.trace("Response receive has started on channel {}. Took {} ms", ctx.channel(), responseReceiveStart);
        response = (HttpResponse) in;
      }
      if (in instanceof HttpContent) {
        recognized = true;
        metrics.delayBetweenChunkReceiveInMs.update(currentChunkReceiveTime - lastChunkReceiveTime);
        chunksReceived++;
        ByteBuf content = ((HttpContent) in).content();
        int bytesReceivedThisTime = content.readableBytes();
        sizeReceived += bytesReceivedThisTime;
        metrics.bytesReceiveRate.mark(bytesReceivedThisTime);
        content.getBytes(0, responseBytes, bytesReceivedThisTime);
        if (in instanceof LastHttpContent) {
          long requestRoundTripTime = currentChunkReceiveTime - requestStartTime;
          logger.trace(
              "Final content received on channel {}. Took {} ms. Total chunks received - {}. Total size received - {}",
              ctx.channel(), requestRoundTripTime, chunksReceived, sizeReceived);
          metrics.requestRoundTripTimeInMs.update(requestRoundTripTime);
          if (httpMethod.equals(HttpMethod.POST)) {
            if (response.status().equals(HttpResponseStatus.CREATED)) {
              blobId = response.headers().get(RestUtils.Headers.LOCATION).substring(1);
              posted.put(blobId, HttpUtil.getContentLength(request));
              blobIds.add(blobId);
            } else {
              metrics.postError.inc();
              logger.error("POST failed!");
            }
          } else if (httpMethod.equals(HttpMethod.GET)) {
            metrics.getContentSizeInBytes.update(sizeReceived);
            metrics.getChunkCount.update(chunksReceived);
            if (response.status().equals(HttpResponseStatus.OK)) {
              if (couldReceiveOK) {
                byte[] recvdBlob = responseBytes.toByteArray();
                if (posted.containsKey(blobId)) {
                  long size = posted.get(blobId);
                  for (int i = 0; i < size; i++) {
                    if (blob[i] != recvdBlob[i]) {
                      metrics.getError.inc();
                      logger.error("Data from GET of {} does not match!", blobId);
                      break;
                    }
                  }
                } else {
                  ByteBuffer srcBuf = getBlobDataFromStore().getStream().getByteBuffer();
                  if (srcBuf.remaining() != recvdBlob.length) {
                    metrics.getError.inc();
                    logger.error("Data from GET of {} does not match!", blobId);
                  } else {
                    for (byte aRecvdBlob : recvdBlob) {
                      if (srcBuf.get() != aRecvdBlob) {
                        metrics.getError.inc();
                        logger.error("Data from GET of {} does not match!", blobId);
                        break;
                      }
                    }
                  }
                }
              } else {
                metrics.getError.inc();
                logger.error("GET of {} should have returned GONE", blobId);
              }
            } else if (response.status().equals(HttpResponseStatus.GONE)) {
              // ensure that the 410 response is warranted
              if (couldReceiveOK && !deleteCandidates.contains(blobId) && isOKRightNow(blobId)) {
                metrics.getError.inc();
                IndexValue value = index.findKey(new BlobId(blobId, clusterMap));
                logger.error("GET of {} failed with code {}. Index Value is {}!", blobId, response.status(), value);
              }
            } else if (response.status().equals(HttpResponseStatus.NOT_FOUND)) {
              IndexValue value = index.findKey(new BlobId(blobId, clusterMap));
              if (!index.isExpired(value)) {
                metrics.getError.inc();
                logger.error("GET of {} failed with code {}. Index Value is {}!", blobId, response.status(), value);
              }
            } else if (posted.containsKey(blobId) || !getBlobDataFromStore().getBlobType()
                .equals(BlobType.MetadataBlob)) {
              metrics.getError.inc();
              logger.error("GET of {} failed with code {}!", blobId, response.status());
            }
          } else if (httpMethod.equals(HttpMethod.DELETE)) {
            if (response.status().equals(HttpResponseStatus.ACCEPTED)) {
              deleted.add(blobId);
            } else {
              metrics.deleteError.inc();
              logger.error("DELETE of {} failed!", blobId);
            }
            deleteCandidates.remove(blobId);
          }
          if (HttpUtil.isKeepAlive(response) && isRunning) {
            logger.trace("Sending new request on channel {}", ctx.channel());
            sendRequest(ctx);
          } else if (!isRunning) {
            logger.info("Closing channel {} because StoreTrafficGenerator has been shutdown", ctx.channel());
            ctx.close();
          } else {
            metrics.requestResponseError.inc();
            logger.error("Channel {} not kept alive. Last response status was {}", ctx.channel(), response.status());
            ctx.close();
          }
        }
      }
      if (!recognized) {
        throw new IllegalStateException("Unexpected HttpObject - " + in.getClass());
      }
      lastChunkReceiveTime = currentChunkReceiveTime;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      logger.info("Channel {} running {} inactive", ctx.channel(), httpMethod);
      ctx.close();
      if (isRunning && (!verificationMode || blobId != null)) {
        metrics.unexpectedDisconnectionError.inc();
        logger.info("Creating a new channel to keep up concurrency");
        if (!verificationMode) {
          methodSelectorQueue.offer(httpMethod);
        }
        b.connect().addListener(channelConnectListener);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (!verificationMode || blobId != null) {
        metrics.requestResponseError.inc();
        logger.error("Exception caught on channel {} while processing request/response", ctx.channel(), cause);
        ctx.close();
      }
    }

    /**
     * Sends the request according to the configuration.
     * @param ctx the {@link ChannelHandlerContext} to use to send the request.
     * @throws Exception
     */
    private void sendRequest(ChannelHandlerContext ctx) throws Exception {
      throttlers.get(httpMethod).maybeThrottle(1);
      requestId++;
      long globalId = totalRequestCount.incrementAndGet();
      logger.trace("Sending request with global ID {} and local ID {} on channel {}", globalId, requestId,
          ctx.channel());
      reset();
      metrics.requestRate.mark();
      ctx.writeAndFlush(request);
      if (request.method().equals(HttpMethod.POST)) {
        ctx.writeAndFlush(chunkedInput);
      }
      logger.trace("Request {} scheduled to be sent on channel {}", requestId, ctx.channel());
    }

    /**
     * Resets all state in preparation for the next request-response.
     * @throws IOException
     * @throws StoreException
     */
    private void reset() throws IOException, StoreException {
      blobId = null;
      httpMethod = verificationMode ? HttpMethod.GET : httpMethod;
      if (httpMethod.equals(HttpMethod.POST)) {
        request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        int actualSize = random.nextInt(generatorConfig.postBlobMaxSize + 1);
        chunkedInput = new HttpChunkedInput(new ResponseHandler.RepeatedBytesInput(actualSize));
        HttpUtil.setContentLength(request, actualSize);
        request.headers().add(RestUtils.Headers.BLOB_SIZE, actualSize);
        request.headers().add(RestUtils.Headers.SERVICE_ID, "StoreTrafficGenerator");
        request.headers().add(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      } else if (httpMethod.equals(HttpMethod.GET)) {
        if (verificationMode) {
          blobId = blobsToVerify.poll();
          if (blobId == null) {
            throw new IllegalArgumentException("There are no more blobs to verify");
          }
        } else {
          blobId = blobIds.get(random.nextInt(blobIds.size()));
        }
        couldReceiveOK = isOKRightNow(blobId);
        request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, blobId);
      } else if (httpMethod.equals(HttpMethod.DELETE)) {
        while (blobId == null) {
          blobId = blobIds.get(random.nextInt(blobIds.size()));
          if (!posted.containsKey(blobId)) {
            // make sure this is not an expired blob. After compaction, expired blobs cannot be deleted
            IndexValue value = index.findKey(new BlobId(blobId, clusterMap));
            blobId = index.isExpired(value) ? null : blobId;
          }
        }
        deleteCandidates.add(blobId);
        request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, blobId);
      }
      responseBytes.reset();
      chunksReceived = 0;
      sizeReceived = 0;
      lastChunkReceiveTime = 0;
      requestStartTime = System.currentTimeMillis();
      response = null;
    }

    private boolean isOKRightNow(String blobId) throws IOException, StoreException {
      boolean isOKRightNow;
      if (deleted.contains(blobId)) {
        isOKRightNow = false;
      } else if (posted.containsKey(blobId)) {
        isOKRightNow = true;
      } else {
        IndexValue value = index.findKey(new BlobId(blobId, clusterMap));
        isOKRightNow = !(value.isFlagSet(IndexValue.Flags.Delete_Index) || index.isExpired(value));
      }
      return isOKRightNow;
    }

    /**
     * @return the {@link BlobData} that contains details about the blob.
     */
    private BlobData getBlobDataFromStore() throws IOException, MessageFormatException, StoreException {
      BlobReadOptions options =
          index.getBlobReadInfo(new BlobId(blobId, clusterMap), EnumSet.allOf(StoreGetOptions.class));
      StoreMessageReadSet readSet = new StoreMessageReadSet(Arrays.asList(options));
      MessageFormatSend send =
          new MessageFormatSend(readSet, MessageFormatFlags.Blob, new MessageFormatMetrics(metricRegistry),
              storeKeyFactory);
      ByteBuffer buffer = ByteBuffer.allocate((int) send.sizeInBytes());
      ByteBufferChannel channel = new ByteBufferChannel(buffer);
      while (!send.isSendComplete()) {
        send.writeTo(channel);
      }
      return MessageFormatRecord.deserializeBlob(new ByteArrayInputStream(buffer.array()));
    }

    /**
     * Returns a chunk with the same data again and again until a fixed size is reached.
     */
    private class RepeatedBytesInput implements ChunkedInput<ByteBuf> {
      private final int totalSize;
      private final AtomicBoolean metricRecorded = new AtomicBoolean(false);
      private final Logger logger = LoggerFactory.getLogger(getClass());

      private boolean done = false;
      private long startTime;

      /**
       * Creates an instance that repeatedly sends the same chunk up to the configured size.
       */
      RepeatedBytesInput(int totalSize) {
        if (totalSize < 0) {
          throw new IllegalArgumentException("Invalid total size");
        }
        this.totalSize = totalSize;
      }

      @Override
      public boolean isEndOfInput() {
        if (done && metricRecorded.compareAndSet(false, true)) {
          long postChunksTime = System.currentTimeMillis() - startTime;
          metrics.postChunksTimeInMs.update(postChunksTime);
          logger.debug("Took {} ms to POST the blob of size {}", postChunksTime, totalSize);
        }
        return done;
      }

      @Override
      public void close() {
        // nothing to do.
      }

      @Override
      public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
      }

      @Override
      public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        ByteBuf buf = null;
        if (!done) {
          startTime = System.currentTimeMillis();
          buf = Unpooled.wrappedBuffer(blob, 0, totalSize);
          done = true;
        }
        return buf;
      }

      @Override
      public long length() {
        return totalSize;
      }

      @Override
      public long progress() {
        return done ? totalSize : 0;
      }
    }
  }

  /**
   * Channel connection listener that prints error if channel could not be connected.
   */
  private class ChannelConnectListener implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        metrics.connectError.inc();
        logger.error("Channel {} to {}:{} could not be connected.", future.channel(), generatorConfig.host,
            generatorConfig.port, future.cause());
      }
    }
  }

  /**
   * Metrics that track peformance.
   */
  private static class StoreTrafficGeneratorMetrics {
    final Meter bytesReceiveRate;
    final Meter channelCreationRate;
    final Meter requestRate;

    final Histogram delayBetweenChunkReceiveInMs;
    final Histogram getContentSizeInBytes;
    final Histogram getChunkCount;
    final Histogram postChunksTimeInMs;
    final Histogram requestRoundTripTimeInMs;
    final Histogram timeToFirstResponseChunkInMs;

    final Counter postError;
    final Counter getError;
    final Counter deleteError;

    final Counter connectError;
    final Counter requestResponseError;
    final Counter unexpectedDisconnectionError;

    /**
     * Creates an instance of PerfClientMetrics.
     * @param metricRegistry the {@link MetricRegistry} instance to use.
     */
    StoreTrafficGeneratorMetrics(MetricRegistry metricRegistry) {
      bytesReceiveRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "BytesReceiveRate"));
      channelCreationRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "ChannelCreationRate"));
      requestRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "RequestRate"));

      delayBetweenChunkReceiveInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "DelayBetweenChunkReceiveInMs"));
      getContentSizeInBytes = metricRegistry.histogram(
          MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "GetContentSizeInBytes"));
      getChunkCount =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "GetChunkCount"));
      postChunksTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "PostChunksTimeInMs"));
      requestRoundTripTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "RequestRoundTripTimeInMs"));
      timeToFirstResponseChunkInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "TimeToFirstResponseChunkInMs"));

      postError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "PostError"));
      getError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "GetError"));
      deleteError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "DeleteError"));

      connectError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "ConnectError"));
      requestResponseError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "RequestResponseError"));
      unexpectedDisconnectionError =
          metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "UnexpectedDisconnectionError"));
    }
  }
}
