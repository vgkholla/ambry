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
package com.github.ambry.router;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;

import static org.junit.Assert.*;


/**
 * Class with helper methods for testing the router.
 */
class RouterTestHelpers {
  private static final int AWAIT_TIMEOUT_SECONDS = 10;

  /**
   * Test whether two {@link BlobProperties} have the same fields
   * @return true if the fields are equivalent in the two {@link BlobProperties}
   */
  static boolean haveEquivalentFields(BlobProperties a, BlobProperties b) {
    return a.getServiceId().equals(b.getServiceId()) && a.getOwnerId().equals(b.getOwnerId()) && a.getContentType()
        .equals(b.getContentType()) && a.isPrivate() == b.isPrivate()
        && a.getTimeToLiveInSeconds() == b.getTimeToLiveInSeconds()
        && a.getCreationTimeInMs() == b.getCreationTimeInMs() && a.getAccountId() == b.getAccountId()
        && a.getContainerId() == b.getContainerId() && a.isEncrypted() == b.isEncrypted();
  }

  /**
   * Test that an operation returns a certain result when servers return error codes corresponding
   * to {@code serverErrorCodeCounts}
   * @param serverErrorCodeCounts A map from {@link ServerErrorCode}s to a count of how many servers should be set to
   *                              that error code.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(Map<ServerErrorCode, Integer> serverErrorCodeCounts, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    List<ServerErrorCode> serverErrorCodes = new ArrayList<>();
    for (Map.Entry<ServerErrorCode, Integer> entry : serverErrorCodeCounts.entrySet()) {
      for (int j = 0; j < entry.getValue(); j++) {
        serverErrorCodes.add(entry.getKey());
      }
    }
    Collections.shuffle(serverErrorCodes);
    testWithErrorCodes(serverErrorCodes.toArray(new ServerErrorCode[0]), serverLayout, expectedError, errorCodeChecker);
  }

  /**
   * Test that an operation returns a certain result when each server in the layout returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.  If
   *                                there are more values in this array than there are servers, an exception is thrown.
   *                                If there are fewer, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, MockServerLayout serverLayout,
      RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker) throws Exception {
    setServerErrorCodes(serverErrorCodesInOrder, serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Test that an operation returns a certain result when each server in a partition returns a certain error code.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are more values in this array than
   *                                there are servers containing the partition, an exception is thrown. If there are
   *                                fewer, the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @param expectedError The {@link RouterErrorCode} expected, or null if no error is expected.
   * @param errorCodeChecker Performs the checks that ensure that the expected {@link RouterErrorCode} is returned .
   * @throws Exception
   */
  static void testWithErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout, RouterErrorCode expectedError, ErrorCodeChecker errorCodeChecker)
      throws Exception {
    setServerErrorCodes(serverErrorCodesInOrder, partition, serverLayout);
    errorCodeChecker.testAndAssert(expectedError);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Set the servers in the specified layout to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers in {@code serverLayout}.
   *                                If there are fewer error codes in this array than there are servers,
   *                                the rest of the servers are set to {@link ServerErrorCode#No_Error}
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, MockServerLayout serverLayout) {
    Collection<MockServer> servers = serverLayout.getMockServers();
    if (serverErrorCodesInOrder.length > servers.size()) {
      throw new IllegalArgumentException("More server error codes provided than servers in cluster");
    }
    int i = 0;
    for (MockServer server : servers) {
      if (i < serverErrorCodesInOrder.length) {
        server.setServerErrorForAllRequests(serverErrorCodesInOrder[i++]);
      } else {
        server.setServerErrorForAllRequests(ServerErrorCode.No_Error);
      }
    }
  }

  /**
   * Set the servers containing the specified partition to respond with the designated error codes.
   * @param serverErrorCodesInOrder The error codes to set in the order of the servers returned by
   *                                {@link PartitionId#getReplicaIds()}. If there are fewer error codes in this array
   *                                than there are servers containing the partition, the rest of the servers are set to
   *                                {@link ServerErrorCode#No_Error}
   * @param partition The partition contained by the servers to set error codes on.
   * @param serverLayout A {@link MockServerLayout} that is used to find the {@link MockServer}s to set error codes on.
   * @throws IllegalArgumentException If there are more error codes in the input array then there are servers.
   */
  static void setServerErrorCodes(ServerErrorCode[] serverErrorCodesInOrder, PartitionId partition,
      MockServerLayout serverLayout) {
    List<? extends ReplicaId> replicas = partition.getReplicaIds();
    if (serverErrorCodesInOrder.length > replicas.size()) {
      throw new IllegalArgumentException("More server error codes provided than replicas in partition");
    }
    int i = 0;
    for (ReplicaId replica : partition.getReplicaIds()) {
      DataNodeId node = replica.getDataNodeId();
      MockServer mockServer = serverLayout.getMockServer(node.getHostname(), node.getPort());
      mockServer.setServerErrorForAllRequests(serverErrorCodesInOrder[i++]);
    }
  }

  /**
   * Asserts that expected threads are not running after the router is closed.
   */
  static void assertCloseCleanup(NonBlockingRouter router) {
    router.close();
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Check that a router operation has failed with a router exception with the specified error code.
   * @param future the {@link Future} for the router operation
   * @param callback the {@link TestCallback} for the router operation. Can be {@code null}
   * @param expectedError the expected {@link RouterErrorCode}
   */
  static <T> void assertFailureAndCheckErrorCode(Future<T> future, TestCallback<T> callback,
      RouterErrorCode expectedError) {
    try {
      callback.getLatch().await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      future.get(1, TimeUnit.SECONDS);
      fail("Operation should be unsuccessful. Exception is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be " + expectedError + " (future)", expectedError,
          ((RouterException) e.getCause()).getErrorCode());
      if (callback != null) {
        assertEquals("RouterErrorCode should be " + expectedError + " (callback)", expectedError,
            ((RouterException) callback.getException()).getErrorCode());
        assertNull("Result should be null", callback.getResult());
      }
    }
  }

  /**
   * Implement this interface to provide {@link #testWithErrorCodes} with custom verification logic.
   */
  interface ErrorCodeChecker {
    /**
     * Test and make assertions related to the expected error code.
     * @param expectedError The {@link RouterErrorCode} that an operation is expected to report, or {@code null} if no
     *                      error is expected.
     * @throws Exception
     */
    void testAndAssert(RouterErrorCode expectedError) throws Exception;
  }

  /**
   * Class that is a helper callback to simply store the result and exception
   * @param <T> the type of the callback required
   */
  static class TestCallback<T> implements Callback<T> {
    private final CountDownLatch latch;
    private T result;
    private Exception exception;

    /**
     * Default ctor
     */
    TestCallback() {
      this(new CountDownLatch(1));
    }

    /**
     * @param latch latch that will be counted down when callback is received.
     */
    TestCallback(CountDownLatch latch) {
      this.latch = latch;
    }

    /**
     * @return the result received by this callback
     */
    T getResult() {
      return result;
    }

    /**
     * @return the {@link Exception} received by this callback
     */
    Exception getException() {
      return exception;
    }

    /**
     * @return the {@link CountDownLatch} associated with this callback
     */
    CountDownLatch getLatch() {
      return latch;
    }

    @Override
    public void onCompletion(T result, Exception exception) {
      this.result = result;
      this.exception = exception;
      latch.countDown();
    }
  }
}

