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
package com.github.ambry.server;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.server.RouterServerTestFramework.*;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.server.RouterServerTestFramework.*;


@RunWith(Parameterized.class)
public class RouterServerPlaintextTest {
  private static MockCluster plaintextCluster;
  private static RouterServerTestFramework testFramework;
  private static MetricRegistry routerMetricRegistry;
  private static long plainTextSendBytesCountBeforeTest;
  private static long plainTextReceiveBytesCountBeforeTest;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Instantiates {@link RouterServerPlaintextTest}
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public RouterServerPlaintextTest(boolean testEncryption) {
    testFramework.setTestEncryption(testEncryption);
  }

  @BeforeClass
  public static void initializeTests() throws Exception {
    MockNotificationSystem notificationSystem = new MockNotificationSystem(9);
    Properties properties = getRouterProperties("DC1");
    plaintextCluster = new MockCluster(notificationSystem, false, SystemTime.getInstance());
    plaintextCluster.startServers();
    MockClusterMap routerClusterMap = plaintextCluster.getClusterMap();
    // MockClusterMap returns a new registry by default. This is to ensure that each node (server, router and so on,
    // get a different registry. But at this point all server nodes have been initialized, and we want the router and
    // its components, which are going to be created, to use the same registry.
    routerClusterMap.createAndSetPermanentMetricRegistry();
    testFramework = new RouterServerTestFramework(properties, routerClusterMap, notificationSystem);
    routerMetricRegistry = routerClusterMap.getMetricRegistry();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Before
  public void before() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    plainTextSendBytesCountBeforeTest = meters.get(plaintextSendBytesMetricName).getCount();
    plainTextReceiveBytesCountBeforeTest = meters.get(plaintextReceiveBytesMetricName).getCount();
  }

  @After
  public void after() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    Assert.assertTrue("Router should have sent over Plain Text",
        meters.get(plaintextSendBytesMetricName).getCount() != plainTextSendBytesCountBeforeTest);
    Assert.assertTrue("Router should have received over Plain Text",
        meters.get(plaintextReceiveBytesMetricName).getCount() != plainTextReceiveBytesCountBeforeTest);
    Assert.assertTrue("Router should not have sent over SSL", meters.get(sslSendBytesMetricName).getCount() == 0);
    Assert.assertTrue("Router should not have received over SSL",
        meters.get(sslReceiveBytesMetricName).getCount() == 0);
  }

  /**
   * Test that the non blocking router can handle a large number of concurrent (small blob) operations without errors.
   * This test creates chains of operations without waiting for previous operations to finish.
   * @throws Exception
   */
  @Test
  public void interleavedOperationsTest() throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 20; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      switch (i % 3) {
        case 0:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED_SUCCESS);
          operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
          break;
        case 1:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED_SUCCESS);
          operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
          break;
        case 2:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          break;
      }
      int blobSize = random.nextInt(100 * 1024);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  /**
   * Test that the non-blocking router can handle simple operation chains where each chain is completed before
   * the next one runs. This means that operations on only one blob are being dealt with at a time.
   * @throws Exception
   */
  @Test
  public void nonInterleavedOperationsTest() throws Exception {
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.GET);
      operations.add(OperationType.DELETE);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED);
      operations.add(OperationType.GET_DELETED);
      operations.add(OperationType.GET_DELETED_SUCCESS);
      operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
      int blobSize = random.nextInt(100 * 1024);
      testFramework.checkOperationChains(
          Collections.singletonList(testFramework.startOperationChain(blobSize, i, operations)));
    }
  }

  /**
   * Test that the non-blocking router can handle multi-chunk blobs.
   * @throws Exception
   */
  @Test
  public void largeBlobTest() throws Exception {
    final int blobSize = RouterServerTestFramework.CHUNK_SIZE * 2 + 1;
    List<OperationChain> opChains = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.GET);
      operations.add(OperationType.DELETE);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED);
      operations.add(OperationType.GET_DELETED);
      operations.add(OperationType.GET_DELETED_SUCCESS);
      operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}
