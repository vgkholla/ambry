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
package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageStoreRecovery;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


class MockId extends StoreKey {

  private String id;
  private static final int Id_Size_In_Bytes = 2;

  MockId(String id) {
    this.id = id;
  }

  MockId(DataInputStream stream) throws IOException {
    id = Utils.readShortString(stream);
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer idBuf = ByteBuffer.allocate(Id_Size_In_Bytes + id.length());
    idBuf.putShort((short) id.length());
    idBuf.put(id.getBytes());
    return idBuf.array();
  }

  @Override
  public String getID() {
    return toString();
  }

  @Override
  public String getLongForm() {
    return getID();
  }

  @Override
  public short sizeInBytes() {
    return (short) (Id_Size_In_Bytes + id.length());
  }

  @Override
  public int compareTo(StoreKey o) {
    if (o == null) {
      throw new NullPointerException();
    }
    MockId otherId = (MockId) o;
    return id.compareTo(otherId.id);
  }

  @Override
  public int hashCode() {
    return Utils.hashcode(new Object[]{id});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MockId other = (MockId) obj;

    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }
}

class MockIdFactory implements StoreKeyFactory {

  @Override
  public StoreKey getStoreKey(DataInputStream value) throws IOException {
    return new MockId(value);
  }
}

public class BlobStoreRecoveryTest {

  private static class ReadImp implements Read {

    StoreKey[] keys = {new MockId("id1"), new MockId("id2"), new MockId("id3"), new MockId("id4")};
    long expectedExpirationTimeMs = 0;
    private ByteBuffer buffer;

    void initialize(int headerSize, int alignment) throws MessageFormatException, IOException {
      // write 3 new blob messages, and delete update messages. write the last
      // message that is partial
      byte[] usermetadata = new byte[2000];
      byte[] blob = new byte[4000];
      TestUtils.RANDOM.nextBytes(usermetadata);
      TestUtils.RANDOM.nextBytes(blob);
      short accountId = Utils.getRandomShort(TestUtils.RANDOM);
      short containerId = Utils.getRandomShort(TestUtils.RANDOM);
      long deletionTimeMs = SystemTime.getInstance().milliseconds() + TestUtils.RANDOM.nextInt();
      long size = headerSize;

      // 1st message
      BlobProperties blobProperties = new BlobProperties(4000, "test", "mem1", "img", false, 9999);
      expectedExpirationTimeMs =
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
      PutMessageFormatInputStream msg1 =
          new PutMessageFormatInputStream(keys[0], blobProperties, ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      size = TestUtils.getAlignedOffset(size, alignment) + msg1.getSize();

      // 2nd message
      PutMessageFormatInputStream msg2 =
          new PutMessageFormatInputStream(keys[1], new BlobProperties(4000, "test"), ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      size = TestUtils.getAlignedOffset(size, alignment) + msg2.getSize();

      // 3rd message
      PutMessageFormatInputStream msg3 =
          new PutMessageFormatInputStream(keys[2], new BlobProperties(4000, "test"), ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      size = TestUtils.getAlignedOffset(size, alignment) + msg3.getSize();

      // 4th message
      DeleteMessageFormatInputStream msg4 =
          new DeleteMessageFormatInputStream(keys[1], accountId, containerId, deletionTimeMs);
      size = TestUtils.getAlignedOffset(size, alignment) + msg4.getSize();

      // 5th message
      PutMessageFormatInputStream msg5 =
          new PutMessageFormatInputStream(keys[3], new BlobProperties(4000, "test"), ByteBuffer.wrap(usermetadata),
              new ByteBufferInputStream(ByteBuffer.wrap(blob)), 4000);
      // only half of message 5 is written
      size = TestUtils.getAlignedOffset(size, alignment) + msg5.getSize() / 2;

      buffer = ByteBuffer.allocate((int) size);

      // move the pointer to past headerSize bytes
      buffer.position(headerSize);
      writeToBuffer(msg1, (int) msg1.getSize(), alignment);
      writeToBuffer(msg2, (int) msg2.getSize(), alignment);
      writeToBuffer(msg3, (int) msg3.getSize(), alignment);
      writeToBuffer(msg4, (int) msg4.getSize(), alignment);
      writeToBuffer(msg5, (int) (msg5.getSize() / 2), alignment);
    }

    private void writeToBuffer(MessageFormatInputStream stream, int sizeToWrite, int alignment) throws IOException {
      int sizeWritten = 0;
      int position = (int) TestUtils.getAlignedOffset(buffer.position(), alignment);
      while (sizeWritten < sizeToWrite) {
        int read = stream.read(buffer.array(), position, sizeToWrite);
        sizeWritten += read;
        buffer.position(position + sizeWritten);
      }
    }

    @Override
    public void readInto(ByteBuffer bufferToWrite, long position) throws IOException {
      bufferToWrite.put(buffer.array(), (int) position, bufferToWrite.remaining());
    }

    public int getSize() {
      return buffer.capacity();
    }
  }

  @Test
  public void recoveryTest() throws MessageFormatException, IOException {
    // TODO (Gopal): also need to check sizes of the messages.
    MessageStoreRecovery recovery = new BlobStoreRecovery();
    for (int headerSize = 0; headerSize < 50; headerSize += 7) {
      for (int alignment = 1; alignment < 20; alignment++) {
        ReadImp readrecovery = new ReadImp();
        readrecovery.initialize(headerSize, alignment);
        List<MessageInfo> recoveredMessages =
            recovery.recover(readrecovery, headerSize, readrecovery.getSize(), alignment, new MockIdFactory());
        Assert.assertEquals(4, recoveredMessages.size());
        Assert.assertEquals(readrecovery.keys[0], recoveredMessages.get(0).getStoreKey());
        Assert.assertEquals(readrecovery.expectedExpirationTimeMs, recoveredMessages.get(0).getExpirationTimeInMs());
        Assert.assertEquals(readrecovery.keys[1], recoveredMessages.get(1).getStoreKey());
        Assert.assertEquals(readrecovery.keys[2], recoveredMessages.get(2).getStoreKey());
        Assert.assertEquals(readrecovery.keys[1], recoveredMessages.get(3).getStoreKey());
        Assert.assertTrue(recoveredMessages.get(3).isDeleted());
      }
    }
  }
}
