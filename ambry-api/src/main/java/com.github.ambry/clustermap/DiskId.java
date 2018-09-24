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
package com.github.ambry.clustermap;

import org.json.JSONObject;


/**
 * A DiskId stores {@link ReplicaId}s. Each DiskId is hosted on one specific {@link DataNodeId}. Each DiskId is uniquely
 * identified by its DataNodeId and mount path (the path to this Disk's device on its DataNode).
 */
public interface DiskId extends Resource {

  /**
   * Gets the absolute path to the mounted device
   *
   * @return absolute mount path.
   */
  String getMountPath();

  /**
   * Gets the state of the DiskId.
   *
   * @return state of the DiskId.
   */
  HardwareState getState();

  /**
   * Gets the raw capacity in bytes for this DiskId.
   *
   * @return the raw capacity in bytes
   */
  long getRawCapacityInBytes();

  /**
   * @return a snapshot which includes information that the implementation considers relevant.
   */
  JSONObject getSnapshot();
}
