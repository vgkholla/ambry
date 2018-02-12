/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.NettyClient.ResponseParts;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.handler.codec.http.multipart.MemoryFileUpload;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for Ambry frontend.
 */
@RunWith(Parameterized.class)
public class FrontendIntegrationTest {
  private static final int PLAINTEXT_SERVER_PORT = 1174;
  private static final int SSL_SERVER_PORT = 1175;
  private static final int MAX_MULTIPART_POST_SIZE_BYTES = 10 * 10 * 1024;
  private static final ClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final VerifiableProperties SSL_CLIENT_VERIFIABLE_PROPS;
  private static final FrontendConfig FRONTEND_CONFIG;
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      File trustStoreFile = File.createTempFile("truststore", ".jks");
      trustStoreFile.deleteOnExit();
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVProps(trustStoreFile);
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RestServer ambryRestServer = null;
  private static NettyClient plaintextNettyClient = null;
  private static NettyClient sslNettyClient = null;

  private final NettyClient nettyClient;

  /**
   * Running it many times so that keep-alive bugs are caught.
   * We also want to test using both the SSL and plaintext ports.
   * @return a list of arrays that represent the constructor arguments for that run of the test.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    List<Object[]> parameters = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      parameters.add(new Object[]{false});
      parameters.add(new Object[]{true});
    }
    return parameters;
  }

  /**
   * Sets up an Ambry frontend server.
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    ambryRestServer = new RestServer(FRONTEND_VERIFIABLE_PROPS, CLUSTER_MAP, new LoggingNotificationSystem(),
        new SSLFactory(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    ambryRestServer.start();
    plaintextNettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
    sslNettyClient =
        new NettyClient("localhost", SSL_SERVER_PORT, new SSLFactory(new SSLConfig(SSL_CLIENT_VERIFIABLE_PROPS)));
  }

  /**
   * Shuts down the Ambry frontend server.
   */
  @AfterClass
  public static void teardown() {
    if (plaintextNettyClient != null) {
      plaintextNettyClient.close();
    }
    if (sslNettyClient != null) {
      sslNettyClient.close();
    }
    if (ambryRestServer != null) {
      ambryRestServer.shutdown();
    }
  }

  /**
   * @param useSSL {@code true} if SSL should be tested.
   */
  public FrontendIntegrationTest(boolean useSSL) {
    nettyClient = useSSL ? sslNettyClient : plaintextNettyClient;
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadDeleteTest() throws Exception {
    // add some accounts
    Account refAccount = ACCOUNT_SERVICE.createAndAddRandomAccount();
    Container publicContainer = refAccount.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    Container privateContainer = refAccount.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
    int refContentSize = FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes * 3;
    for (int i = 0; i < 2; i++) {
      ACCOUNT_SERVICE.createAndAddRandomAccount();
    }

    // with valid account and containers
    for (Account account : ACCOUNT_SERVICE.getAllAccounts()) {
      if (account.getId() != Account.UNKNOWN_ACCOUNT_ID) {
        for (Container container : account.getAllContainers()) {
          doPostGetHeadDeleteTest(refContentSize, account, container, account.getName(), !container.isCacheable(),
              account.getName(), container.getName(), false);
        }
      }
    }
    // valid account and container names but only serviceId passed as part of POST
    doPostGetHeadDeleteTest(refContentSize, null, null, refAccount.getName(), false, refAccount.getName(),
        publicContainer.getName(), false);
    doPostGetHeadDeleteTest(refContentSize, null, null, refAccount.getName(), true, refAccount.getName(),
        privateContainer.getName(), false);
    // unrecognized serviceId
    doPostGetHeadDeleteTest(refContentSize, null, null, "unknown_service_id", false, null, null, false);
    doPostGetHeadDeleteTest(refContentSize, null, null, "unknown_service_id", true, null, null, false);
    // different sizes
    for (int contentSize : new int[]{0, FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes
        - 1, FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes, refContentSize}) {
      doPostGetHeadDeleteTest(contentSize, refAccount, publicContainer, refAccount.getName(),
          !publicContainer.isCacheable(), refAccount.getName(), publicContainer.getName(), false);
    }
  }

  /**
   * Tests multipart POST and verifies it via GET operations.
   * @throws Exception
   */
  @Test
  public void multipartPostGetHeadTest() throws Exception {
    Account refAccount = ACCOUNT_SERVICE.createAndAddRandomAccount();
    Container refContainer = refAccount.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    doPostGetHeadDeleteTest(0, refAccount, refContainer, refAccount.getName(), !refContainer.isCacheable(),
        refAccount.getName(), refContainer.getName(), true);
    doPostGetHeadDeleteTest(FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes * 3, refAccount, refContainer,
        refAccount.getName(), !refContainer.isCacheable(), refAccount.getName(), refContainer.getName(), true);

    // failure case
    // size of content being POSTed is higher than what is allowed via multipart/form-data
    long maxAllowedSizeBytes = new NettyConfig(FRONTEND_VERIFIABLE_PROPS).nettyMultipartPostMaxSizeBytes;
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes((int) maxAllowedSizeBytes + 1));
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeadersForPut(headers, 7200, !refContainer.isCacheable(), refAccount.getName(), "application/octet-stream",
        null, refAccount.getName(), refContainer.getName());
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", headers);
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, content, ByteBuffer.allocate(0));
    ResponseParts responseParts = nettyClient.sendRequest(encoder.finalizeRequest(), encoder, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertFalse("Channel should not be active", HttpUtil.isKeepAlive(response));
  }

  /*
   * Tests health check request
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void healthCheckRequestTest() throws ExecutionException, InterruptedException, IOException {
    FullHttpRequest httpRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthCheck", Unpooled.buffer(0));
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    final String expectedResponseBody = "GOOD";
    ByteBuffer content = getContent(responseParts.queue, expectedResponseBody.length());
    assertEquals("GET content does not match original content", expectedResponseBody, new String(content.array()));
  }

  /**
   * Tests {@link RestUtils.SubResource#Replicas} requests
   * <p/>
   * For each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created. The replica list returned from
   * server is checked for equality against a locally obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasTest() throws Exception {
    List<? extends PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          ClusterMapUtils.UNKNOWN_DATACENTER_ID, Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
          partitionId, false);
      FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
          blobId.getID() + "/" + RestUtils.SubResource.Replicas, Unpooled.buffer(0));
      ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
      HttpResponse response = getHttpResponse(responseParts);
      assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
      ByteBuffer content = getContent(responseParts.queue, HttpUtil.getContentLength(response));
      JSONObject responseJson = new JSONObject(new String(content.array()));
      String returnedReplicasStr = responseJson.getString(GetReplicasHandler.REPLICAS_KEY).replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests the handling of {@link Operations#GET_SIGNED_URL} requests.
   * @throws Exception
   */
  @Test
  public void getAndUseSignedUrlTest() throws Exception {
    Account account = ACCOUNT_SERVICE.createAndAddRandomAccount();
    Container container = account.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
    // setup
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(10));
    long blobTtl = 7200;
    String serviceId = "getAndUseSignedUrlTest";
    String contentType = "application/octet-stream";
    String ownerId = "getAndUseSignedUrlTest";
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    setAmbryHeadersForPut(headers, blobTtl, !container.isCacheable(), serviceId, contentType, ownerId,
        account.getName(), container.getName());
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");

    // POST
    // Get signed URL
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, Operations.GET_SIGNED_URL, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertNotNull("There should be a response from the server", response);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    String signedPostUrl = response.headers().get(RestUtils.Headers.SIGNED_URL);
    assertNotNull("Did not get a signed POST URL", signedPostUrl);
    discardContent(responseParts.queue, 1);

    // Use signed URL to POST
    URI uri = new URI(signedPostUrl);
    httpRequest = buildRequest(HttpMethod.POST, uri.getPath() + "?" + uri.getQuery(), null, content);
    responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    String blobId = verifyPostAndReturnBlobId(responseParts);

    // verify POST
    headers.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
    getBlobAndVerify(blobId, null, GetOption.None, headers, !container.isCacheable(), content);
    getBlobInfoAndVerify(blobId, GetOption.None, headers, !container.isCacheable(), account.getName(),
        container.getName(), null);

    // GET
    // Get signed URL
    HttpHeaders getHeaders = new DefaultHttpHeaders();
    getHeaders.add(RestUtils.Headers.URL_TYPE, RestMethod.GET.name());
    blobId = blobId.startsWith("/") ? blobId.substring(1) : blobId;
    getHeaders.add(RestUtils.Headers.BLOB_ID, blobId);
    httpRequest = buildRequest(HttpMethod.GET, Operations.GET_SIGNED_URL, getHeaders, null);
    responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    response = getHttpResponse(responseParts);
    assertNotNull("There should be a response from the server", response);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    String signedGetUrl = response.headers().get(RestUtils.Headers.SIGNED_URL);
    assertNotNull("Did not get a signed GET URL", signedGetUrl);
    discardContent(responseParts.queue, 1);

    // Use URL to GET blob
    uri = new URI(signedGetUrl);
    httpRequest = buildRequest(HttpMethod.GET, uri.getPath() + "?" + uri.getQuery(), null, null);
    responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    verifyGetBlobResponse(responseParts, null, headers, !container.isCacheable(), content);
  }

  /**
   * Tests for handling of {@link HttpMethod#OPTIONS}.
   * @throws Exception
   */
  @Test
  public void optionsTest() throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.OPTIONS, "", null, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    assertEquals("Unexpected value for " + HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
        FRONTEND_CONFIG.frontendOptionsAllowMethods,
        response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS));
    assertEquals("Unexpected value for " + HttpHeaderNames.ACCESS_CONTROL_MAX_AGE,
        FRONTEND_CONFIG.frontendOptionsValiditySeconds,
        Long.parseLong(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE)));
  }

  // helpers
  // general

  /**
   * Method to easily create a request.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link HttpHeaders} object. Can be null.
   * @param content the content that accompanies the request. Can be null.
   * @return A {@link FullHttpRequest} object that defines the request required by the input.
   */
  private FullHttpRequest buildRequest(HttpMethod httpMethod, String uri, HttpHeaders headers, ByteBuffer content) {
    ByteBuf contentBuf;
    if (content != null) {
      contentBuf = Unpooled.wrappedBuffer(content);
    } else {
      contentBuf = Unpooled.buffer(0);
    }
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, contentBuf);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    if (HttpMethod.POST.equals(httpMethod) && !HttpUtil.isContentLengthSet(httpRequest)) {
      HttpUtil.setTransferEncodingChunked(httpRequest, true);
    }
    return httpRequest;
  }

  /**
   * Combines all the parts in {@code contents} into one {@link ByteBuffer}.
   * @param contents the content of the response.
   * @param expectedContentLength the length of the contents in bytes.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  private ByteBuffer getContent(Queue<HttpObject> contents, long expectedContentLength) {
    ByteBuffer buffer = ByteBuffer.allocate((int) expectedContentLength);
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      buffer.put(content.content().nioBuffer());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(content);
    }
    assertEquals("Content length did not match expected", expectedContentLength, buffer.position());
    assertTrue("End marker was not found", endMarkerFound);
    buffer.flip();
    return buffer;
  }

  /**
   * Verifies that no content has been sent as part of the response or readable bytes is equivalent to 0
   * @param contents the content of the response.
   */
  private void assertNoContent(Queue<HttpObject> contents) {
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      assertEquals("No content expected ", 0, content.content().readableBytes());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(content);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
  }

  /**
   * Discards all the content in {@code contents}.
   * @param contents the content to discard.
   * @param expectedDiscardCount the number of {@link HttpObject}s that are expected to discarded.
   */
  private void discardContent(Queue<HttpObject> contents, int expectedDiscardCount) {
    assertEquals("Objects that will be discarded differ from expected", expectedDiscardCount, contents.size());
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(object);
    }
    assertTrue("There should have been an end marker", endMarkerFound);
  }

  // BeforeClass helpers

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.put("rest.server.blob.storage.service.factory",
        "com.github.ambry.frontend.AmbryBlobStorageServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("netty.server.port", Integer.toString(PLAINTEXT_SERVER_PORT));
    properties.put("netty.server.ssl.port", Integer.toString(SSL_SERVER_PORT));
    properties.put("netty.server.enable.ssl", "true");
    // to test that backpressure does not impede correct operation.
    properties.put("netty.server.request.buffer.watermark", "1");
    // to test that multipart requests over a certain size fail
    properties.put("netty.multipart.post.max.size.bytes", Long.toString(MAX_MULTIPART_POST_SIZE_BYTES));
    TestSSLUtils.addSSLProperties(properties, "", SSLFactory.Mode.SERVER, trustStoreFile, "frontend");
    properties.put("frontend.account.service.factory", "com.github.ambry.account.InMemAccountServiceFactory");
    // add key for singleKeyManagementService
    properties.put("kms.default.container.key", TestUtils.getRandomKey(32));
    return new VerifiableProperties(properties);
  }

  // postGetHeadDeleteTest() and multipartPostGetHeadTest() helpers

  /**
   * Utility to test blob POST, GET, HEAD and DELETE operations for a specified size
   * @param contentSize the size of the blob to be tested
   * @param toPostAccount the {@link Account} to use in post headers. Can be {@code null} if only using service ID.
   * @param toPostContainer the {@link Container} to use in post headers. Can be {@code null} if only using service ID.
   * @param serviceId the serviceId to use for the POST
   * @param isPrivate the isPrivate flag to pass as part of the POST
   * @param expectedAccountName the expected account name in some response.
   * @param expectedContainerName the expected container name in some responses.
   * @param multipartPost {@code true} if multipart POST is desired, {@code false} otherwise.
   * @throws Exception
   */
  private void doPostGetHeadDeleteTest(int contentSize, Account toPostAccount, Container toPostContainer,
      String serviceId, boolean isPrivate, String expectedAccountName, String expectedContainerName,
      boolean multipartPost) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentSize));
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    String accountNameInPost = toPostAccount != null ? toPostAccount.getName() : null;
    String containerNameInPost = toPostContainer != null ? toPostContainer.getName() : null;
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeadersForPut(headers, 7200, isPrivate, serviceId, contentType, ownerId, accountNameInPost,
        containerNameInPost);
    String blobId;
    byte[] usermetadata = null;
    if (multipartPost) {
      usermetadata = UtilsTest.getRandomString(32).getBytes();
      blobId = multipartPostBlobAndVerify(headers, content, ByteBuffer.wrap(usermetadata));
    } else {
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
      blobId = postBlobAndVerify(headers, content);
    }
    headers.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
    getBlobAndVerify(blobId, null, null, headers, isPrivate, content);
    getHeadAndVerify(blobId, null, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    getBlobAndVerify(blobId, null, GetOption.None, headers, isPrivate, content);
    getHeadAndVerify(blobId, null, GetOption.None, headers, isPrivate, expectedAccountName, expectedContainerName);
    ByteRange range = ByteRange.fromLastNBytes(ThreadLocalRandom.current().nextLong(content.capacity() + 1));
    getBlobAndVerify(blobId, range, null, headers, isPrivate, content);
    getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    if (contentSize > 0) {
      range = ByteRange.fromStartOffset(ThreadLocalRandom.current().nextLong(content.capacity()));
      getBlobAndVerify(blobId, range, null, headers, isPrivate, content);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
      long random1 = ThreadLocalRandom.current().nextLong(content.capacity());
      long random2 = ThreadLocalRandom.current().nextLong(content.capacity());
      range = ByteRange.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
      getBlobAndVerify(blobId, range, null, headers, isPrivate, content);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    }
    getNotModifiedBlobAndVerify(blobId, null, isPrivate);
    getUserMetadataAndVerify(blobId, null, headers, usermetadata);
    getBlobInfoAndVerify(blobId, null, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, content,
        usermetadata);
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param httpHeaders the {@link HttpHeaders} where the headers should be set.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @param targetAccountName sets the {@link RestUtils.Headers#TARGET_ACCOUNT_NAME} header. Can be {@code null}.
   * @param targetContainerName sets the {@link RestUtils.Headers#TARGET_CONTAINER_NAME} header. Can be {@code null}.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   */
  private void setAmbryHeadersForPut(HttpHeaders httpHeaders, long ttlInSecs, boolean isPrivate, String serviceId,
      String contentType, String ownerId, String targetAccountName, String targetContainerName) {
    if (httpHeaders != null && serviceId != null && contentType != null) {
      if (ttlInSecs > -1) {
        httpHeaders.add(RestUtils.Headers.TTL, ttlInSecs);
      }
      httpHeaders.add(RestUtils.Headers.SERVICE_ID, serviceId);
      httpHeaders.add(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
      if (targetAccountName != null) {
        httpHeaders.add(RestUtils.Headers.TARGET_ACCOUNT_NAME, targetAccountName);
      }
      if (targetContainerName != null) {
        httpHeaders.add(RestUtils.Headers.TARGET_CONTAINER_NAME, targetContainerName);
      } else {
        httpHeaders.add(RestUtils.Headers.PRIVATE, isPrivate);
      }
      if (ownerId != null) {
        httpHeaders.add(RestUtils.Headers.OWNER_ID, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private String postBlobAndVerify(HttpHeaders headers, ByteBuffer content)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, "/", headers, content);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    return verifyPostAndReturnBlobId(responseParts);
  }

  /**
   * Verifies a POST and returns the blob ID.
   * @param responseParts the response received from the server.
   * @returnn the blob ID of the blob.
   */
  private String verifyPostAndReturnBlobId(ResponseParts responseParts) {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.status());
    assertTrue("No Date header", response.headers().getTimeMillis(HttpHeaderNames.DATE, -1) != -1);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
    String blobId = response.headers().get(HttpHeaderNames.LOCATION, null);
    assertNotNull("Blob ID from POST should not be null", blobId);
    discardContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param range the {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param expectedContent the expected content of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobAndVerify(String blobId, ByteRange range, GetOption getOption, HttpHeaders expectedHeaders,
      boolean isPrivate, ByteBuffer expectedContent)
      throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (range != null) {
      headers.add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    verifyGetBlobResponse(responseParts, range, expectedHeaders, isPrivate, expectedContent);
  }

  /**
   * Verifies the GET blob response.
   * @param responseParts the response received from the server.
   * @param range the {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param expectedContent the expected content of the blob.
   * @throws RestServiceException
   */
  private void verifyGetBlobResponse(ResponseParts responseParts, ByteRange range, HttpHeaders expectedHeaders,
      boolean isPrivate, ByteBuffer expectedContent) throws RestServiceException {
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertEquals("Accept-Ranges not set correctly", "bytes", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    byte[] expectedContentArray = expectedContent.array();
    if (range != null) {
      long blobSize = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
      assertEquals("Content-Range header not set correctly",
          RestUtils.buildContentRangeAndLength(range, blobSize).getFirst(),
          response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      ByteRange resolvedRange = range.toResolvedByteRange(blobSize);
      expectedContentArray = Arrays.copyOfRange(expectedContentArray, (int) resolvedRange.getStartOffset(),
          (int) resolvedRange.getEndOffset() + 1);
    } else {
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    }
    if (expectedContentArray.length < FRONTEND_CONFIG.frontendChunkedGetResponseThresholdInBytes) {
      assertEquals("Content-length not as expected", expectedContentArray.length, HttpUtil.getContentLength(response));
    }
    verifyCacheHeaders(isPrivate, response);
    byte[] responseContentArray = getContent(responseParts.queue, expectedContentArray.length).array();
    assertArrayEquals("GET content does not match original content", expectedContentArray, responseContentArray);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @param getOption the options to use while getting the blob.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @throws Exception
   */
  private void getNotModifiedBlobAndVerify(String blobId, GetOption getOption, boolean isPrivate) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    headers.add(RestUtils.Headers.IF_MODIFIED_SINCE, new Date());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.NOT_MODIFIED, response.status());
    assertNotNull("Date header should be set", response.headers().get(RestUtils.Headers.DATE));
    assertNotNull("Last-Modified header should be set", response.headers().get("Last-Modified"));
    assertNull("Content-Length should not be set", response.headers().get(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Accept-Ranges should not be set", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", response.headers().get(RestUtils.Headers.CONTENT_TYPE));
    verifyCacheHeaders(isPrivate, response);
    assertNoContent(responseParts.queue);
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getUserMetadataAndVerify(String blobId, GetOption getOption, HttpHeaders expectedHeaders,
      byte[] usermetadata) throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    checkCommonGetHeadHeaders(response.headers());
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts.queue);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobInfoAndVerify(String blobId, GetOption getOption, HttpHeaders expectedHeaders, boolean isPrivate,
      String accountName, String containerName, byte[] usermetadata) throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    checkCommonGetHeadHeaders(response.headers());
    verifyBlobProperties(expectedHeaders, isPrivate, response);
    verifyAccountAndContainerHeaders(accountName, containerName, response);
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts.queue);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param range the {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in the response.
   * @param containerName the expected container name in the response.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getHeadAndVerify(String blobId, ByteRange range, GetOption getOption, HttpHeaders expectedHeaders,
      boolean isPrivate, String accountName, String containerName)
      throws ExecutionException, InterruptedException, RestServiceException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (range != null) {
      headers.add(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    if (getOption != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status",
        range == null ? HttpResponseStatus.OK : HttpResponseStatus.PARTIAL_CONTENT, response.status());
    checkCommonGetHeadHeaders(response.headers());
    long contentLength = Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE));
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength);
      assertEquals("Content-Range header not set correctly", rangeAndLength.getFirst(),
          response.headers().get(RestUtils.Headers.CONTENT_RANGE));
      contentLength = rangeAndLength.getSecond();
    } else {
      assertNull("Content-Range header should not be set", response.headers().get(RestUtils.Headers.CONTENT_RANGE));
    }
    assertEquals("Accept-Ranges not set correctly", "bytes", response.headers().get(RestUtils.Headers.ACCEPT_RANGES));
    assertEquals(RestUtils.Headers.CONTENT_LENGTH + " does not match expected", contentLength,
        HttpUtil.getContentLength(response));
    assertEquals(RestUtils.Headers.CONTENT_TYPE + " does not match " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    verifyBlobProperties(expectedHeaders, isPrivate, response);
    verifyAccountAndContainerHeaders(accountName, containerName, response);
    discardContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param response the {@link HttpResponse} that contains the headers.
   */
  private void verifyBlobProperties(HttpHeaders expectedHeaders, boolean isPrivate, HttpResponse response) {
    assertEquals("Blob size does not match", Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)),
        Long.parseLong(response.headers().get(RestUtils.Headers.BLOB_SIZE)));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        response.headers().get(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", isPrivate,
        Boolean.valueOf(response.headers().get(RestUtils.Headers.PRIVATE)));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        response.headers().get(RestUtils.Headers.CREATION_TIME, null) != null);
    if (Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          response.headers().get(RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          response.headers().get(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verifies the account and container headers in the response
   * @param accountName the expected account name in {@code response}.
   * @param containerName the expected container name in {@code response}.
   * @param response the response received from Ambry.
   */
  private void verifyAccountAndContainerHeaders(String accountName, String containerName, HttpResponse response) {
    String accountNameInResponse = response.headers().get(RestUtils.Headers.TARGET_ACCOUNT_NAME);
    String containerNameInResponse = response.headers().get(RestUtils.Headers.TARGET_CONTAINER_NAME);
    if (accountName != null && containerName != null) {
      assertEquals("Account name does not match that to which blob was uploaded", accountName, accountNameInResponse);
      assertEquals("Container name does not match that to which blob was uploaded", containerName,
          containerNameInResponse);
    } else {
      assertNull("Response should not have any account name - has " + accountNameInResponse, accountNameInResponse);
      assertNull("Response should not have any container name - has " + containerNameInResponse,
          containerNameInResponse);
    }
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param response the {@link HttpResponse} which contains the headers of the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @param content the content accompanying the response.
   */
  private void verifyUserMetadata(HttpHeaders expectedHeaders, HttpResponse response, byte[] usermetadata,
      Queue<HttpObject> content) {
    if (usermetadata == null) {
      assertEquals("Content-Length is not 0", 0, HttpUtil.getContentLength(response));
      for (Map.Entry<String, String> header : expectedHeaders) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertEquals("Value for " + key + " does not match in user metadata", header.getValue(),
              response.headers().get(key));
        }
      }
      for (Map.Entry<String, String> header : response.headers()) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertTrue("Key " + key + " does not exist in expected headers", expectedHeaders.contains(key));
        }
      }
      discardContent(content, 1);
    } else {
      assertEquals("Content-Length is not as expected", usermetadata.length, HttpUtil.getContentLength(response));
      byte[] receivedMetadata = getContent(content, HttpUtil.getContentLength(response)).array();
      assertArrayEquals("User metadata does not match original", usermetadata, receivedMetadata);
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void deleteBlobAndVerify(String blobId) throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Verifies that the right response code is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @param expectedHeaders the expected headers in the response if the right options are provided.
   * @param isPrivate {@code true} if the blob is expected to be private
   * @param accountName the expected account name in {@code response}.
   * @param containerName the expected container name in {@code response}.
   * @param expectedContent the expected content of the blob if the right options are provided.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws Exception
   */
  private void verifyOperationsAfterDelete(String blobId, HttpHeaders expectedHeaders, boolean isPrivate,
      String accountName, String containerName, ByteBuffer expectedContent, byte[] usermetadata) throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders().add(RestUtils.Headers.GET_OPTION, GetOption.None.toString());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);
    httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);
    httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);

    GetOption[] options = {GetOption.Include_Deleted_Blobs, GetOption.Include_All};
    for (GetOption option : options) {
      getBlobAndVerify(blobId, null, option, expectedHeaders, isPrivate, expectedContent);
      getNotModifiedBlobAndVerify(blobId, option, isPrivate);
      getUserMetadataAndVerify(blobId, option, expectedHeaders, usermetadata);
      getBlobInfoAndVerify(blobId, option, expectedHeaders, isPrivate, accountName, containerName, usermetadata);
      getHeadAndVerify(blobId, null, option, expectedHeaders, isPrivate, accountName, containerName);
    }
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param httpRequest the {@link FullHttpRequest} to send to the server.
   * @param expectedStatusCode the expected {@link HttpResponseStatus}.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void verifyDeleted(FullHttpRequest httpRequest, HttpResponseStatus expectedStatusCode)
      throws ExecutionException, InterruptedException {
    ResponseParts responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = getHttpResponse(responseParts);
    assertEquals("Unexpected response status", expectedStatusCode, response.status());
    assertTrue("No Date header", response.headers().get(HttpHeaderNames.DATE, null) != null);
    discardContent(responseParts.queue, 1);
    assertTrue("Channel should be active", HttpUtil.isKeepAlive(response));
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   */
  private void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders) {
    assertTrue("No Date header", receivedHeaders.get(HttpHeaderNames.DATE) != null);
    assertTrue("No Last-Modified header", receivedHeaders.get(HttpHeaderNames.LAST_MODIFIED) != null);
  }

  /**
   * Verifies that the right cache headers are returned.
   * @param isPrivate {@code true} if the blob is private, {@code false} if not.
   * @param response the {@link HttpResponse}.
   */
  private void verifyCacheHeaders(boolean isPrivate, HttpResponse response) {
    if (isPrivate) {
      Assert.assertEquals("Cache-Control value not as expected", "private, no-cache, no-store, proxy-revalidate",
          response.headers().get(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertEquals("Pragma value not as expected", "no-cache", response.headers().get(RestUtils.Headers.PRAGMA));
    } else {
      String expiresValue = response.headers().get(RestUtils.Headers.EXPIRES);
      assertNotNull("Expires value should be non null", expiresValue);
      assertTrue("Expires value should be in future",
          RestUtils.getTimeFromDateString(expiresValue) > System.currentTimeMillis());
      Assert.assertEquals("Cache-Control value not as expected",
          "max-age=" + FRONTEND_CONFIG.frontendCacheValiditySeconds,
          response.headers().get(RestUtils.Headers.CACHE_CONTROL));
      Assert.assertNull("Pragma value should not have been set", response.headers().get(RestUtils.Headers.PRAGMA));
    }
  }

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @param usermetadata the {@link ByteBuffer} that represents user metadata
   * @return the blob ID of the blob.
   * @throws Exception
   */
  private String multipartPostBlobAndVerify(HttpHeaders headers, ByteBuffer content, ByteBuffer usermetadata)
      throws Exception {
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", headers);
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, content, usermetadata);
    ResponseParts responseParts = nettyClient.sendRequest(encoder.finalizeRequest(), encoder, null).get();
    return verifyPostAndReturnBlobId(responseParts);
  }

  /**
   * Creates a {@link HttpPostRequestEncoder} that encodes the given {@code request} and {@code blobContent}.
   * @param request the {@link HttpRequest} containing headers and other metadata about the request.
   * @param blobContent the {@link ByteBuffer} that represents the content of the blob.
   * @param usermetadata the {@link ByteBuffer} that represents user metadata
   * @return a {@link HttpPostRequestEncoder} that can encode the {@code request} and {@code blobContent}.
   * @throws HttpPostRequestEncoder.ErrorDataEncoderException
   * @throws IOException
   */
  private HttpPostRequestEncoder createEncoder(HttpRequest request, ByteBuffer blobContent, ByteBuffer usermetadata)
      throws HttpPostRequestEncoder.ErrorDataEncoderException, IOException {
    HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
    HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(httpDataFactory, request, true);
    FileUpload fileUpload = new MemoryFileUpload(RestUtils.MultipartPost.BLOB_PART, RestUtils.MultipartPost.BLOB_PART,
        "application/octet-stream", "", Charset.forName("UTF-8"), blobContent.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(blobContent));
    encoder.addBodyHttpData(fileUpload);
    fileUpload =
        new MemoryFileUpload(RestUtils.MultipartPost.USER_METADATA_PART, RestUtils.MultipartPost.USER_METADATA_PART,
            "application/octet-stream", "", Charset.forName("UTF-8"), usermetadata.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(usermetadata));
    encoder.addBodyHttpData(fileUpload);
    return encoder;
  }

  /**
   * @param responseParts a {@link ResponseParts}.
   * @return the first response part, which should be a {@link HttpResponse}.
   * @throws IllegalStateException if the response part queue is empty.
   */
  private HttpResponse getHttpResponse(ResponseParts responseParts) {
    HttpResponse httpResponse = (HttpResponse) responseParts.queue.poll();
    if (httpResponse == null) {
      throw new IllegalStateException(
          "Should have received response. completion context: " + responseParts.completionContext);
    }
    return httpResponse;
  }
}
