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
package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOptions;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.InMemoryRouterFactory;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import junit.framework.Assert;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for Admin.
 */
@RunWith(Parameterized.class)
public class AdminIntegrationTest {
  private static final int SERVER_PORT = 16503;
  private static final ClusterMap CLUSTER_MAP;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RestServer adminRestServer = null;
  private static NettyClient nettyClient = null;
  private static InMemoryRouter router = null;

  /**
   * Running it many times so that keep-alive bugs are caught.
   * @return an array representing the number of times to run.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[5][0]);
  }

  /**
   * Sets up an Admin server.
   * @throws Exception
   */
  @BeforeClass
  public static void setup()
      throws Exception {
    adminRestServer = new RestServer(buildAdminVProps(), CLUSTER_MAP, new LoggingNotificationSystem());
    router = InMemoryRouterFactory.getLatestInstance();
    adminRestServer.start();
    nettyClient = new NettyClient("localhost", SERVER_PORT);
  }

  /**
   * Shuts down the Admin server.
   */
  @AfterClass
  public static void teardown() {
    if (nettyClient != null) {
      nettyClient.close();
    }
    if (adminRestServer != null) {
      adminRestServer.shutdown();
    }
  }

  /**
   * Tests blob GET, HEAD and DELETE operations with blobs posted as both non multipart and multipart.
   * @throws Exception
   */
  @Test
  public void getHeadDeleteTest()
      throws Exception {
    doGetHeadDeleteTest(0, false);
    doGetHeadDeleteTest(0, true);

    doGetHeadDeleteTest(1024, false);
    doGetHeadDeleteTest(1024, true);

    doGetHeadDeleteTest(8192, false);
    doGetHeadDeleteTest(8192, true);

    doGetHeadDeleteTest(10000, false);
    doGetHeadDeleteTest(8192, true);
  }

  /*
   * Tests health check request
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void healtCheckRequestTest()
      throws ExecutionException, InterruptedException, IOException {
    FullHttpRequest httpRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthCheck", Unpooled.buffer(0));
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    ByteBuffer content = getContent(response, responseParts);
    assertEquals("GET content does not match original content", "GOOD", new String(content.array()));
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
    return httpRequest;
  }

  /**
   * Combines all the parts in {@code contents} into one {@link ByteBuffer}.
   * @param response the {@link HttpResponse} containing headers.
   * @param contents the content of the response.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  private ByteBuffer getContent(HttpResponse response, Queue<HttpObject> contents) {
    long contentLength = HttpHeaders.getContentLength(response, -1);
    if (contentLength == -1) {
      contentLength = HttpHeaders.getIntHeader(response, RestUtils.Headers.BLOB_SIZE, 0);
    }
    ByteBuffer buffer = ByteBuffer.allocate((int) contentLength);
    boolean endMarkerFound = false;
    for (HttpObject object : contents) {
      assertFalse("There should have been no more data after the end marker was found", endMarkerFound);
      HttpContent content = (HttpContent) object;
      buffer.put(content.content().nioBuffer());
      endMarkerFound = object instanceof LastHttpContent;
      ReferenceCountUtil.release(content);
    }
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
      Assert.assertEquals("No content expected ", 0, content.content().readableBytes());
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
   * Builds properties required to start a {@link RestServer} as an Admin server.
   * @return a {@link VerifiableProperties} with the parameters for an Admin server.
   */
  private static VerifiableProperties buildAdminVProps() {
    Properties properties = new Properties();
    properties.put("rest.server.blob.storage.service.factory", "com.github.ambry.admin.AdminBlobStorageServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("netty.server.port", Integer.toString(SERVER_PORT));
    return new VerifiableProperties(properties);
  }

  // getHeadDeleteTest() helpers

  /**
   * Utility to test blob GET, HEAD and DELETE operations for a specified size
   * @param contentSize the size of the blob to be tested
   * @param multipartPost {@code true} if multipart POST is desired, {@code false} otherwise.
   * @throws Exception
   */
  private void doGetHeadDeleteTest(int contentSize, boolean multipartPost)
      throws Exception {
    ByteBuffer content = ByteBuffer.wrap(RestTestUtils.getRandomBytes(contentSize));
    String serviceId = "getHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "getHeadDeleteOwnerID";
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeaders(headers, content.capacity(), 7200, false, serviceId, contentType, ownerId);
    headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.capacity());
    String blobId;
    byte[] usermetadata = null;
    if (multipartPost) {
      usermetadata = UtilsTest.getRandomString(32).getBytes();
    } else {
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
      headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    }
    blobId = putBlob(headers, content, usermetadata);
    getBlobAndVerify(blobId, null, headers, content);
    getNotModifiedBlobAndVerify(blobId, null);
    getUserMetadataAndVerify(blobId, null, headers, usermetadata);
    getBlobInfoAndVerify(blobId, null, headers, usermetadata);
    getHeadAndVerify(blobId, null, headers);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId, headers, content, usermetadata);
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param httpHeaders the {@link HttpHeaders} where the headers should be set.
   * @param contentLength sets the {@link RestUtils.Headers#BLOB_SIZE} header. Required.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   * @throws org.json.JSONException
   */
  private void setAmbryHeaders(HttpHeaders httpHeaders, long contentLength, long ttlInSecs, boolean isPrivate,
      String serviceId, String contentType, String ownerId)
      throws JSONException {
    if (httpHeaders != null && contentLength >= 0 && ttlInSecs >= -1 && serviceId != null && contentType != null) {
      httpHeaders.add(RestUtils.Headers.BLOB_SIZE, contentLength);
      httpHeaders.add(RestUtils.Headers.TTL, ttlInSecs);
      httpHeaders.add(RestUtils.Headers.PRIVATE, isPrivate);
      httpHeaders.add(RestUtils.Headers.SERVICE_ID, serviceId);
      httpHeaders.add(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
      if (ownerId != null) {
        httpHeaders.add(RestUtils.Headers.OWNER_ID, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Puts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers required.
   * @param content the content of the blob.
   * @param usermetadata the usermetadata associated with the blob if mimicking multipart POST. {@code null} otherwise.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  private String putBlob(HttpHeaders headers, ByteBuffer content, byte[] usermetadata)
      throws Exception {
    Map<String, Object> args = new HashMap<>();
    for (Map.Entry<String, String> entry : headers.entries()) {
      args.put(entry.getKey(), entry.getValue());
    }
    BlobProperties blobProperties = RestUtils.buildBlobProperties(args);
    if (usermetadata == null) {
      usermetadata = RestUtils.buildUsermetadata(args);
    }
    String blobId = router.putBlob(blobProperties, usermetadata, new ByteBufferReadableStreamChannel(content)).get();
    if (blobId == null) {
      fail("putBlobInRouter did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param getOptions the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobAndVerify(String blobId, GetOptions getOptions, HttpHeaders expectedHeaders,
      ByteBuffer expectedContent)
      throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOptions != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOptions.toString());
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers());
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    ByteBuffer responseContent = getContent(response, responseParts);
    assertArrayEquals("GET content does not match original content", expectedContent.array(), responseContent.array());
    assertTrue("Channel should be active", HttpHeaders.isKeepAlive(response));
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @param getOptions the options to use while getting the blob.
   * @throws Exception
   */
  private void getNotModifiedBlobAndVerify(String blobId, GetOptions getOptions)
      throws Exception {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOptions != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOptions.toString());
    }
    headers.add(RestUtils.Headers.IF_MODIFIED_SINCE, new Date());
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.NOT_MODIFIED, response.getStatus());
    assertTrue("No Date header", response.headers().get(RestUtils.Headers.DATE) != null);
    assertNull("No Last-Modified header expected", response.headers().get("Last-Modified"));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        response.headers().get(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", response.headers().get(RestUtils.Headers.CONTENT_TYPE));
    assertNoContent(responseParts);
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOptions the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getUserMetadataAndVerify(String blobId, GetOptions getOptions, HttpHeaders expectedHeaders,
      byte[] usermetadata)
      throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOptions != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOptions.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers());
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts);
    assertTrue("Channel should be active", HttpHeaders.isKeepAlive(response));
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOptions the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getBlobInfoAndVerify(String blobId, GetOptions getOptions, HttpHeaders expectedHeaders,
      byte[] usermetadata)
      throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOptions != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOptions.toString());
    }
    FullHttpRequest httpRequest =
        buildRequest(HttpMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers());
    verifyBlobProperties(expectedHeaders, response);
    verifyUserMetadata(expectedHeaders, response, usermetadata, responseParts);
    assertTrue("Channel should be active", HttpHeaders.isKeepAlive(response));
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOptions the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void getHeadAndVerify(String blobId, GetOptions getOptions, HttpHeaders expectedHeaders)
      throws ExecutionException, InterruptedException {
    HttpHeaders headers = new DefaultHttpHeaders();
    if (getOptions != null) {
      headers.add(RestUtils.Headers.GET_OPTION, getOptions.toString());
    }
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, headers, null);
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers());
    assertEquals(RestUtils.Headers.CONTENT_LENGTH + " does not match " + RestUtils.Headers.BLOB_SIZE,
        Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)), HttpHeaders.getContentLength(response));
    assertEquals(RestUtils.Headers.CONTENT_TYPE + " does not match " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
    verifyBlobProperties(expectedHeaders, response);
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpHeaders.isKeepAlive(response));
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param response the {@link HttpResponse} that contains the headers.
   */
  private void verifyBlobProperties(HttpHeaders expectedHeaders, HttpResponse response) {
    assertEquals("Blob size does not match", Long.parseLong(expectedHeaders.get(RestUtils.Headers.BLOB_SIZE)),
        Long.parseLong(HttpHeaders.getHeader(response, RestUtils.Headers.BLOB_SIZE)));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        HttpHeaders.getHeader(response, RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", expectedHeaders.get(RestUtils.Headers.PRIVATE),
        HttpHeaders.getHeader(response, RestUtils.Headers.PRIVATE));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        HttpHeaders.getHeader(response, RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        HttpHeaders.getHeader(response, RestUtils.Headers.CREATION_TIME, null) != null);
    if (Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          HttpHeaders.getHeader(response, RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          HttpHeaders.getHeader(response, RestUtils.Headers.OWNER_ID));
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
      assertEquals("Content-Length is not 0", 0, HttpHeaders.getContentLength(response));
      for (Map.Entry<String, String> header : expectedHeaders) {
        String key = header.getKey();
        if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
          assertEquals("Value for " + key + " does not match in user metadata", header.getValue(),
              HttpHeaders.getHeader(response, key));
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
      assertEquals("Content-Length is not as expected", usermetadata.length, HttpHeaders.getContentLength(response));
      byte[] receivedMetadata = getContent(response, content).array();
      assertArrayEquals("User metadata does not match original", usermetadata, receivedMetadata);
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void deleteBlobAndVerify(String blobId)
      throws ExecutionException, InterruptedException {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);
  }

  /**
   * Verifies that the right response code is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @param usermetadata if non-null, this is expected to come as the body.
   * @throws Exception
   */
  private void verifyOperationsAfterDelete(String blobId, HttpHeaders expectedHeaders, ByteBuffer expectedContent,
      byte[] usermetadata)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.GONE);

    httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    verifyDeleted(httpRequest, HttpResponseStatus.ACCEPTED);

    GetOptions[] options = {GetOptions.Include_Deleted_Blobs, GetOptions.Include_All};
    for (GetOptions option : options) {
      getBlobAndVerify(blobId, option, expectedHeaders, expectedContent);
      getNotModifiedBlobAndVerify(blobId, option);
      getUserMetadataAndVerify(blobId, option, expectedHeaders, usermetadata);
      getBlobInfoAndVerify(blobId, option, expectedHeaders, usermetadata);
      getHeadAndVerify(blobId, option, expectedHeaders);
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
    Queue<HttpObject> responseParts = nettyClient.sendRequest(httpRequest, null, null).get();
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", expectedStatusCode, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
    discardContent(responseParts, 1);
    assertTrue("Channel should be active", HttpHeaders.isKeepAlive(response));
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   */
  private void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders) {
    assertTrue("No Date header", receivedHeaders.get(HttpHeaders.Names.DATE) != null);
    assertTrue("No Last-Modified header", receivedHeaders.get(HttpHeaders.Names.LAST_MODIFIED) != null);
  }
}
