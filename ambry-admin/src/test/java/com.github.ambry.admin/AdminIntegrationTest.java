package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Utils;
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
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Integration tests for Admin.
 */
public class AdminIntegrationTest {
  private static final String SERVER_HOSTNAME = "localhost";
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

  /**
   * Sets up an admin server.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void setup()
      throws InstantiationException, IOException {
    adminRestServer = new RestServer(buildAdminVProps(), CLUSTER_MAP, new LoggingNotificationSystem());
    adminRestServer.start();
  }

  /**
   * Shuts down the admin server.
   */
  @AfterClass
  public static void teardown() {
    if (adminRestServer != null) {
      adminRestServer.shutdown();
    }
  }

  /**
   * Tests the {@link AdminBlobStorageService#ECHO} operation. Checks to see that the echo matches input text.
   * @throws Exception
   */
  @Test
  public void echoTest()
      throws Exception {
    String inputText = "loremIpsum";
    String uri = AdminBlobStorageService.ECHO + "?" + EchoHandler.TEXT_KEY + "=" + inputText;
    FullHttpRequest request = buildRequest(HttpMethod.GET, uri, null, null);
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(request);
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected status", HttpResponseStatus.OK, response.getStatus());
    assertEquals("Unexpected Content-Type", "application/json",
        HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
    ByteBuffer buffer = getContent(response, responseParts);
    String echoedText = new JSONObject(new String(buffer.array())).getString(EchoHandler.TEXT_KEY);
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  /**
   * Tests the {@link AdminBlobStorageService#GET_REPLICAS_FOR_BLOB_ID} operation.
   * <p/>
   * For the each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created. The string representation
   * is sent to the server as a part of request. The returned replica list is checked for equality against a locally
   * obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasForBlobIdTest()
      throws Exception {
    List<PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      String uri =
          AdminBlobStorageService.GET_REPLICAS_FOR_BLOB_ID + "?" + GetReplicasForBlobIdHandler.BLOB_ID_KEY + "="
              + blobId;
      FullHttpRequest request = buildRequest(HttpMethod.GET, uri, null, null);
      LinkedBlockingQueue<HttpObject> responseParts = sendRequest(request);
      HttpResponse response = (HttpResponse) responseParts.poll();
      assertEquals("Unexpected status", HttpResponseStatus.OK, response.getStatus());
      assertEquals("Unexpected Content-Type", "application/json",
          HttpHeaders.getHeader(response, HttpHeaders.Names.CONTENT_TYPE));
      ByteBuffer buffer = getContent(response, responseParts);
      JSONObject responseObj = new JSONObject(new String(buffer.array()));
      String returnedReplicasStr = responseObj.getString(GetReplicasForBlobIdHandler.REPLICAS_KEY).replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadDeleteTest()
      throws Exception {
    ByteBuffer content = ByteBuffer.wrap(getRandomBytes(1024));
    String serviceId = "postGetHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeaders(headers, content.capacity(), 7200, false, serviceId, contentType, ownerId);
    headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.capacity());

    String blobId = postBlobAndVerify(headers, content);
    getBlobAndVerify(blobId, headers, content);
    getHeadAndVerify(blobId, headers);
    deleteBlobAndVerify(blobId);

    // repeat delete. This should not throw errors.
    deleteBlobAndVerify(blobId);
    // check GET and HEAD after delete.
    verifyGetAndHeadAfterDelete(blobId);
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
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, contentBuf);
    if (headers != null) {
      request.headers().set(headers);
    }
    return request;
  }

  /**
   * Combines all the parts in {@code contents} into one {@link ByteBuffer}.
   * @param response the {@link HttpResponse} containing headers.
   * @param contents the content of the response.
   * @return a {@link ByteBuffer} that contains all the data in {@code contents}.
   */
  private ByteBuffer getContent(HttpResponse response, LinkedBlockingQueue<HttpObject> contents) {
    long contentLength = HttpHeaders.getContentLength(response, -1);
    if (contentLength == -1) {
      contentLength = HttpHeaders.getIntHeader(response, RestUtils.Headers.Blob_Size, 0);
    }
    ByteBuffer buffer = ByteBuffer.allocate((int) contentLength);
    HttpContent content = (HttpContent) contents.poll();
    while (content != null) {
      buffer.put(content.content().nioBuffer());
      ReferenceCountUtil.release(content);
      content = (HttpContent) contents.poll();
    }
    return buffer;
  }

  /**
   * Discards all the content in {@code contents}.
   * @param contents the content to discard.
   */
  private void discardContent(LinkedBlockingQueue<HttpObject> contents) {
    HttpContent content = (HttpContent) contents.poll();
    while (content != null) {
      ReferenceCountUtil.release(content);
      content = (HttpContent) contents.poll();
    }
  }

  /**
   * Uses a {@link NettyClient} to send the provided {@code request} to the server and returns the response, if any.
   * If an exception occurs during the operation, throws the exception.
   * @param request the {@link FullHttpRequest} that needs to be submitted to the server.
   * @return the {@link LinkedBlockingQueue} that will contain the parts of the response.
   * @throws Exception
   */
  private LinkedBlockingQueue<HttpObject> sendRequest(FullHttpRequest request)
      throws Exception {
    NettyClient nettyClient = new NettyClient(SERVER_HOSTNAME, SERVER_PORT, request);
    try {
      LinkedBlockingQueue<HttpObject> responseParts = nettyClient.sendRequest();
      if (!nettyClient.awaitFullResponse(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Did not receive response after waiting for 1 second");
      }
      return responseParts;
    } finally {
      nettyClient.shutdown();
    }
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
    return new VerifiableProperties(properties);
  }

  // postGetHeadDeleteTest() helpers

  /**
   * Gets a byte array of length {@code size} with random bytes.
   * @param size the required length of the random byte array.
   * @return a byte array of length {@code size} with random bytes.
   */
  private byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    new Random().nextBytes(bytes);
    return bytes;
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param httpHeaders the {@link HttpHeaders} where the headers should be set.
   * @param contentLength sets the {@link RestUtils.Headers#Blob_Size} header. Required.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#Private} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#Service_Id} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#Content_Type} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#Owner_Id} header. Optional - if not required, send null.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   */
  private void setAmbryHeaders(HttpHeaders httpHeaders, long contentLength, long ttlInSecs, boolean isPrivate,
      String serviceId, String contentType, String ownerId) {
    if (httpHeaders != null && contentLength >= 0 && ttlInSecs >= -1 && serviceId != null && contentType != null) {
      httpHeaders.add(RestUtils.Headers.Blob_Size, contentLength);
      httpHeaders.add(RestUtils.Headers.TTL, ttlInSecs);
      httpHeaders.add(RestUtils.Headers.Private, isPrivate);
      httpHeaders.add(RestUtils.Headers.Service_Id, serviceId);
      httpHeaders.add(RestUtils.Headers.Content_Type, contentType);
      if (ownerId != null) {
        httpHeaders.add(RestUtils.Headers.Owner_Id, ownerId);
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
   * @throws Exception
   */
  public String postBlobAndVerify(HttpHeaders headers, ByteBuffer content)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.POST, "/", headers, content);
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(httpRequest);
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.CREATED, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
    assertTrue("No " + RestUtils.Headers.Creation_Time,
        HttpHeaders.getHeader(response, RestUtils.Headers.Creation_Time, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpHeaders.getContentLength(response));
    String blobId = HttpHeaders.getHeader(response, HttpHeaders.Names.LOCATION, null);

    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  public void getBlobAndVerify(String blobId, HttpHeaders expectedHeaders, ByteBuffer expectedContent)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(httpRequest);
    HttpResponse response = (HttpResponse) responseParts.poll();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers(), expectedHeaders);
    ByteBuffer responseContent = getContent(response, responseParts);
    assertArrayEquals("GET content does not match original content", expectedContent.array(), responseContent.array());
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getHeadAndVerify(String blobId, HttpHeaders expectedHeaders)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(httpRequest);
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.getStatus());
    checkCommonGetHeadHeaders(response.headers(), expectedHeaders);
    assertEquals("Content-Length does not match blob size",
        Long.parseLong(expectedHeaders.get(RestUtils.Headers.Blob_Size)), HttpHeaders.getContentLength(response));
    assertEquals(RestUtils.Headers.Service_Id + " does not match", expectedHeaders.get(RestUtils.Headers.Service_Id),
        HttpHeaders.getHeader(response, RestUtils.Headers.Service_Id));
    assertEquals(RestUtils.Headers.Private + " does not match", expectedHeaders.get(RestUtils.Headers.Private),
        HttpHeaders.getHeader(response, RestUtils.Headers.Private));
    assertEquals(RestUtils.Headers.Content_Type + " does not match",
        expectedHeaders.get(RestUtils.Headers.Content_Type),
        HttpHeaders.getHeader(response, RestUtils.Headers.Content_Type));
    assertTrue("No " + RestUtils.Headers.Creation_Time,
        HttpHeaders.getHeader(response, RestUtils.Headers.Creation_Time, null) != null);
    if (Long.parseLong(expectedHeaders.get(RestUtils.Headers.TTL)) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL),
          HttpHeaders.getHeader(response, RestUtils.Headers.TTL));
    }
    if (expectedHeaders.contains(RestUtils.Headers.Owner_Id)) {
      assertEquals(RestUtils.Headers.Owner_Id + " does not match", expectedHeaders.get(RestUtils.Headers.Owner_Id),
          HttpHeaders.getHeader(response, RestUtils.Headers.Owner_Id));
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws Exception
   */
  private void deleteBlobAndVerify(String blobId)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.DELETE, blobId, null, null);
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(httpRequest);
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.ACCEPTED, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
    assertEquals("Content-Length is not 0", 0, HttpHeaders.getContentLength(response));
  }

  /**
   * Verifies that the right response code is returned for GET and HEAD once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @throws Exception
   */
  private void verifyGetAndHeadAfterDelete(String blobId)
      throws Exception {
    FullHttpRequest httpRequest = buildRequest(HttpMethod.GET, blobId, null, null);
    verifyOperationAfterDelete(httpRequest);

    httpRequest = buildRequest(HttpMethod.HEAD, blobId, null, null);
    verifyOperationAfterDelete(httpRequest);
  }

  /**
   * Verifies that a request returns the right response code ({@link HttpResponseStatus#GONE}) once the blob has been
   * deleted.
   * @param httpRequest the {@link FullHttpRequest} to send to the server.
   * @throws Exception
   */
  private void verifyOperationAfterDelete(FullHttpRequest httpRequest)
      throws Exception {
    LinkedBlockingQueue<HttpObject> responseParts = sendRequest(httpRequest);
    HttpResponse response = (HttpResponse) responseParts.poll();
    discardContent(responseParts);
    assertEquals("Unexpected response status", HttpResponseStatus.GONE, response.getStatus());
    assertTrue("No Date header", HttpHeaders.getDateHeader(response, HttpHeaders.Names.DATE, null) != null);
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param receivedHeaders the {@link HttpHeaders} that were received.
   * @param expectedHeaders the expected headers.
   */
  private void checkCommonGetHeadHeaders(HttpHeaders receivedHeaders, HttpHeaders expectedHeaders) {
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.Content_Type),
        receivedHeaders.get(HttpHeaders.Names.CONTENT_TYPE));
    assertTrue("No Date header", receivedHeaders.get(HttpHeaders.Names.DATE) != null);
    assertTrue("No Last-Modified header", receivedHeaders.get(HttpHeaders.Names.LAST_MODIFIED) != null);
    assertEquals(RestUtils.Headers.Blob_Size + " does not match", expectedHeaders.get(RestUtils.Headers.Blob_Size),
        receivedHeaders.get(RestUtils.Headers.Blob_Size));
  }
}
