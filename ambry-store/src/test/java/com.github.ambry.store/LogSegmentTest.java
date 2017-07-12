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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Tests for {@link LogSegment}.
 */
@RunWith(Parameterized.class)
public class LogSegmentTest {
  private static final int STANDARD_SEGMENT_SIZE = 1024;
  private static final int VERSION_0_HEADER_SIZE = 18;
  private static final int VERSION_1_HEADER_SIZE = 22;
  private static final Appender BUFFER_APPENDER = (segment, buffer) -> {
    int writeSize = buffer.remaining();
    int written = segment.appendFrom(buffer);
    assertEquals("Size written did not match size of buffer provided", writeSize, written);
  };
  private static final Appender CHANNEL_APPENDER = (segment, buffer) -> {
    int writeSize = buffer.remaining();
    segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeSize);
    assertFalse("The buffer was not completely written", buffer.hasRemaining());
  };

  private final File tempDir;
  private final int alignment;
  private final boolean writeHeader;
  private final StoreMetrics metrics;

  /**
   * Running for different alignments and with and without headers.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{1, true}, {1, false}, {2, true}, {2, false}, {7, true}, {7, false}});
  }

  /**
   * Sets up a temporary directory that can be used.
   * @throws IOException
   */
  public LogSegmentTest(int alignment, boolean writeHeader) throws IOException {
    tempDir = Files.createTempDirectory("logSegmentDir-" + UtilsTest.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    this.alignment = alignment;
    this.writeHeader = writeHeader;
    metrics = new StoreMetrics(tempDir.getName(), new MetricRegistry());
  }

  /**
   * Deletes the temporary directory that was created.
   */
  @After
  public void cleanup() {
    File[] files = tempDir.listFiles();
    if (files != null) {
      for (File file : files) {
        assertTrue("The file [" + file.getAbsolutePath() + "] could not be deleted", file.delete());
      }
    }
    assertTrue("The directory [" + tempDir.getAbsolutePath() + "] could not be deleted", tempDir.delete());
  }

  /**
   * Tests appending and reading to make sure data is written and the data read is consistent with the data written.
   * @throws IOException
   */
  @Test
  public void basicWriteAndReadTest() throws IOException {
    String segmentName = "log_current";
    // current version (or if writeHeader not true)
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long expectedStartOffset = writeHeader ? VERSION_1_HEADER_SIZE : 0;
      doBasicWriteAndReadTest(segment, segmentName, expectedStartOffset, alignment);
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
    // older versions only if writeHeader is true
    if (writeHeader) {
      // version 0
      segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, alignment, false);
      // alignment will be ignored.
      rewriteSegmentHeader(LogSegment.VERSION_0, segment, alignment);
      try {
        File segmentFile = segment.getView().getFirst();
        segment.closeView();
        segment.close();
        segment = new LogSegment(segmentName, segmentFile, metrics);
        verifyGetters(segment, segmentName, STANDARD_SEGMENT_SIZE, 1);
        doBasicWriteAndReadTest(segment, segmentName, VERSION_0_HEADER_SIZE, 1);
      } finally {
        closeSegmentAndDeleteFile(segment);
      }
    }
  }

  /**
   * Verifies getting and closing views and makes sure that data and ref counts are consistent.
   * @throws IOException
   */
  @Test
  public void viewAndRefCountTest() throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      int readSize = 100;
      int viewCount = 5;
      byte[] data = appendRandomData(segment, readSize * viewCount);
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      for (int i = 0; i < viewCount; i++) {
        getAndVerifyView(segment, writeStartOffset, i * readSize, data, i + 1);
      }
      for (int i = 0; i < viewCount; i++) {
        segment.closeView();
        assertEquals("Ref count is not as expected", viewCount - i - 1, segment.refCount());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests setting end offset - makes sure legal values are set correctly and illegal values are rejected.
   * @throws IOException
   */
  @Test
  public void endOffsetTest() throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      int writeSize = 500;
      appendRandomData(segment, writeSize);
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      assertEquals("End offset is not as expected", writeStartOffset + writeSize, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());

      // should be able to set end offset to something >= initial offset and <= file size
      long[] offsetsToSet = {segment.getStartOffset(), segment.sizeInBytes() / 2, segment.sizeInBytes()};
      for (long offset : offsetsToSet) {
        segment.setEndOffset(offset);
        assertEquals("End offset is not equal to what was set", offset, segment.getEndOffset());
        expectedRemainingWritableCapacity =
            STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
        assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
            segment.getRemainingWritableCapacityInBytes());
        assertEquals("File channel positioning is incorrect", offset, segment.getView().getSecond().position());
      }

      // cannot set end offset to illegal values (< initial offset or > file size)
      int[] invalidOffsets = {(int) (segment.getStartOffset() - 1), (int) (segment.sizeInBytes() + 1)};
      for (int offset : invalidOffsets) {
        try {
          segment.setEndOffset(offset);
          fail("Setting log end offset an invalid offset [" + offset + "] should have failed");
        } catch (IllegalArgumentException e) {
          // expected. Nothing to do.
        }
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests {@link LogSegment#appendFrom(ByteBuffer)} and {@link LogSegment#appendFrom(ReadableByteChannel, long)} for
   * various cases.
   * @throws IOException
   */
  @Test
  public void appendTest() throws IOException {
    // buffer append
    doAppendTest(BUFFER_APPENDER);
    // channel append
    doAppendTest(CHANNEL_APPENDER);
  }

  /**
   * Tests {@link LogSegment#readInto(ByteBuffer, long)} for various cases.
   * @throws IOException
   */
  @Test
  public void readTest() throws IOException {
    Random random = new Random();
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      byte[] data = appendRandomData(segment, 2 * STANDARD_SEGMENT_SIZE / 3);
      readAndEnsureMatch(segment, writeStartOffset, data);
      int readCount = 10;
      for (int i = 0; i < readCount; i++) {
        int position = random.nextInt(data.length);
        int size = random.nextInt(data.length - position);
        readAndEnsureMatch(segment, writeStartOffset + position, Arrays.copyOfRange(data, position, position + size));
      }

      // check for position >= endOffset and < data size written to the segment
      long oldEndOffset = segment.getEndOffset();
      // setting end offset to 1/3 * (sizeInBytes - startOffset)
      segment.setEndOffset(segment.getStartOffset() + (segment.sizeInBytes() - segment.getStartOffset()) / 3);
      long readOffset =
          (int) (segment.getEndOffset() + Utils.getRandomLong(TestUtils.RANDOM, oldEndOffset - segment.getEndOffset()));
      long size = Utils.getRandomLong(TestUtils.RANDOM, oldEndOffset - readOffset);
      int position = (int) (readOffset - writeStartOffset);
      readAndEnsureMatch(segment, readOffset, Arrays.copyOfRange(data, position, position + (int) size));
      segment.setEndOffset(oldEndOffset);

      // error scenarios
      ByteBuffer readBuf = ByteBuffer.wrap(new byte[data.length]);
      // data cannot be read at invalid offsets.
      long[] invalidOffsets = {segment.getStartOffset() - 1, segment.sizeInBytes(), segment.sizeInBytes() + 1};
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      for (long invalidOffset : invalidOffsets) {
        try {
          segment.readInto(readBuf, invalidOffset);
          fail("Should have failed to read because position provided is invalid");
        } catch (IndexOutOfBoundsException e) {
          assertEquals("Position of buffer has changed", 0, buffer.position());
        }
      }

      // position + buffer.remaining > sizeInBytes
      long readOverFlowCount = metrics.overflowReadError.getCount();
      readBuf = ByteBuffer.allocate(2);
      position = (int) segment.sizeInBytes() - 1;
      try {
        segment.readInto(readBuf, position);
        fail("Should have failed to read because position + buffer.remaining() > sizeInBytes");
      } catch (IndexOutOfBoundsException e) {
        assertEquals("Read overflow should have been reported", readOverFlowCount + 1,
            metrics.overflowReadError.getCount());
        assertEquals("Position of buffer has changed", 0, readBuf.position());
      }

      segment.close();
      // read after close
      buffer = ByteBuffer.allocate(1);
      try {
        segment.readInto(buffer, writeStartOffset);
        fail("Should have failed to read because segment is closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests {@link LogSegment#writeFrom(ReadableByteChannel, long, long)} for various cases.
   * @throws IOException
   */
  @Test
  public void writeFromTest() throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long writeStartOffset = segment.getStartOffset();
      byte[] bufOne = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 3);
      byte[] bufTwo = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);

      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), writeStartOffset,
          bufOne.length);
      assertEquals("End offset is not as expected", writeStartOffset + bufOne.length, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
      readAndEnsureMatch(segment, writeStartOffset, bufOne);

      // overwrite using bufTwo
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufTwo))), writeStartOffset,
          bufTwo.length);
      assertEquals("End offset is not as expected", writeStartOffset + bufTwo.length, segment.getEndOffset());
      expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
      readAndEnsureMatch(segment, writeStartOffset, bufTwo);

      // overwrite using bufOne
      segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), writeStartOffset,
          bufOne.length);
      // end offset should not have changed
      assertEquals("End offset is not as expected", writeStartOffset + bufTwo.length, segment.getEndOffset());
      expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
      readAndEnsureMatch(segment, writeStartOffset, bufOne);
      readAndEnsureMatch(segment, writeStartOffset + bufOne.length,
          Arrays.copyOfRange(bufTwo, bufOne.length, bufTwo.length));

      // write at random locations
      for (int i = 0; i < 10; i++) {
        long offset = writeStartOffset + Utils.getRandomLong(TestUtils.RANDOM,
            segment.sizeInBytes() - bufOne.length - writeStartOffset);
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(bufOne))), offset,
            bufOne.length);
        readAndEnsureMatch(segment, offset, bufOne);
      }

      // try to overwrite using a channel that won't fit
      ByteBuffer failBuf =
          ByteBuffer.wrap(TestUtils.getRandomBytes((int) (STANDARD_SEGMENT_SIZE - writeStartOffset + 1)));
      long writeOverFlowCount = metrics.overflowWriteError.getCount();
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(failBuf)), writeStartOffset,
            failBuf.remaining());
        fail("WriteFrom should have failed because data won't fit");
      } catch (IndexOutOfBoundsException e) {
        assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
            metrics.overflowWriteError.getCount());
        assertEquals("Position of buffer has changed", 0, failBuf.position());
      }

      // data cannot be written at invalid offsets.
      long[] invalidOffsets = {segment.getStartOffset() - 1, STANDARD_SEGMENT_SIZE, STANDARD_SEGMENT_SIZE + 1};
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      for (long invalidOffset : invalidOffsets) {
        try {
          segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), invalidOffset, buffer.remaining());
          fail("WriteFrom should have failed because offset provided for write is invalid");
        } catch (IndexOutOfBoundsException e) {
          assertEquals("Position of buffer has changed", 0, buffer.position());
        }
      }

      segment.close();
      // ensure that writeFrom fails.
      try {
        segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), writeStartOffset, buffer.remaining());
        fail("WriteFrom should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests for special constructor cases.
   * @throws IOException
   */
  @Test
  public void constructorTest() throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    long expectedStartOffset = writeHeader ? LogSegment.getCurrentVersionHeaderSize() : 0;
    assertEquals("Start offset should be 0 when no headers are written", expectedStartOffset, segment.getStartOffset());
  }

  /**
   * Tests for bad construction cases of {@link LogSegment}.
   * @throws IOException
   */
  @Test
  public void badConstructionTest() throws IOException {
    // try to construct with a file that does not exist.
    String name = "log_non_existent";
    File file = new File(tempDir, name);
    try {
      new LogSegment(name, file, STANDARD_SEGMENT_SIZE, alignment, metrics, writeHeader);
      fail("Construction should have failed because the backing file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    try {
      new LogSegment(name, file, metrics);
      fail("Construction should have failed because the backing file does not exist");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // try to construct with a file that is a directory
    name = tempDir.getName();
    file = new File(tempDir.getParent(), name);
    try {
      new LogSegment(name, file, STANDARD_SEGMENT_SIZE, alignment, metrics, writeHeader);
      fail("Construction should have failed because the backing file is a directory");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    name = tempDir.getName();
    file = new File(tempDir.getParent(), name);
    try {
      new LogSegment(name, file, metrics);
      fail("Construction should have failed because the backing file is a directory");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    if (writeHeader) {
      // unknown version
      LogSegment segment = getSegment("dummy_log", STANDARD_SEGMENT_SIZE, alignment, true);
      file = segment.getView().getFirst();
      byte[] header = getHeader(segment);
      byte savedByte = header[0];
      // mess with version
      header[0] = (byte) (header[0] + 10);
      writeHeader(segment, header);
      try {
        new LogSegment(name, file, metrics);
        fail("Construction should have failed because version is unknown");
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }

      // bad CRC
      // fix version but mess with data after version
      header[0] = savedByte;
      header[2] = header[2] == (byte) 1 ? (byte) 0 : (byte) 1;
      writeHeader(segment, header);
      try {
        new LogSegment(name, file, metrics);
        fail("Construction should have failed because crc check should have failed");
      } catch (IllegalStateException e) {
        // expected. Nothing to do.
      }
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Tests that header size returned by {@link LogSegment#getCurrentVersionHeaderSize()} is correct.
   * @throws IOException
   */
  @Test
  public void getCurrentVersionHeaderSizeTest() throws IOException {
    assumeTrue(writeHeader);
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, true);
    try {
      assertEquals("Current version header size incorrect", segment.getEndOffset(),
          LogSegment.getCurrentVersionHeaderSize());
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Performs writes that are aligned and ensures that there are no "holes" created.
   * @throws IOException
   */
  @Test
  public void alignedWritesTest() throws IOException {
    doAlignedWritesTest(BUFFER_APPENDER);
    doAlignedWritesTest(CHANNEL_APPENDER);
  }

  /**
   * Performs writes that are not aligned and ensures that they are aligned correctly.
   * @throws IOException
   */
  @Test
  public void nonAlignedWritesTest() throws IOException {
    assumeTrue(alignment > 1);
    doNonAlignedWritesTest(BUFFER_APPENDER);
    doNonAlignedWritesTest(CHANNEL_APPENDER);
  }

  /**
   * Tests the case where alignment changes across log segment reloads. The change affects only log segments without
   * headers.
   * @throws IOException
   */
  @Test
  public void changingAlignmentsTest() throws IOException {
    doChangingAlignmentsTest(BUFFER_APPENDER);
    doChangingAlignmentsTest(CHANNEL_APPENDER);
  }

  /**
   * Tests that append fails when there is enough non-aligned space but not enough aligned space.
   * @throws IOException
   */
  @Test
  public void notEnoughAlignedSpaceTest() throws IOException {
    assumeTrue(alignment > 1);
    doNotEnoughAlignedSpaceTest(BUFFER_APPENDER);
    doNotEnoughAlignedSpaceTest(CHANNEL_APPENDER);
  }

  // helpers
  // general

  /**
   * Creates and gets a {@link LogSegment} with a header in the given {@code version}.
   * @param version the version of the header required.
   * @param segment the segment whose header needs to be rewritten (segment
   * @param alignment the alignment for the start offset of every new message written into the segment.
   * @throws IOException
   */
  static void rewriteSegmentHeader(short version, LogSegment segment, int alignment) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(stream);
    dos.writeShort(version);
    dos.writeLong(segment.getCapacityInBytes());
    switch (version) {
      case LogSegment.VERSION_0:
        break;
      case LogSegment.VERSION_1:
        dos.writeInt(alignment);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version: " + version);
    }
    Crc32 crc = new Crc32();
    crc.update(stream.toByteArray(), 0, dos.size());
    dos.writeLong(crc.getValue());
    byte[] header = stream.toByteArray();
    writeHeader(segment, header);
  }

  /**
   * Writes {@code buf} into {@code segment} as a header.
   * @param segment the {@link LogSegment} to write a header for.
   * @param buf the header to write
   * @throws IOException
   */
  private static void writeHeader(LogSegment segment, byte[] buf) throws IOException {
    FileChannel channel = segment.getView().getSecond();
    ByteBuffer buffer = ByteBuffer.wrap(buf);
    while (buffer.hasRemaining()) {
      channel.write(buffer, 0);
    }
  }

  /**
   * Creates and gets a {@link LogSegment} (in the current version if {@code writeHeader} is true).
   * @param segmentName the name of the segment as well as the file backing the segment.
   * @param capacityInBytes the capacity of the file/segment.
   * @param alignment the alignment for the start offset of every new message written into the segment.
   * @param writeHeader if {@code true}, writes headers for the segment.
   * @return instance of {@link LogSegment} that is backed by the file with name {@code segmentName} of capacity
   * {@code capacityInBytes} with alignment {@code alignment}.
   * @throws IOException
   */
  private LogSegment getSegment(String segmentName, long capacityInBytes, int alignment, boolean writeHeader)
      throws IOException {
    File file = new File(tempDir, segmentName);
    if (file.exists()) {
      assertTrue(file.getAbsolutePath() + " already exists and could not be deleted", file.delete());
    }
    assertTrue("Segment file could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    file.deleteOnExit();
    LogSegment segment;
    try (RandomAccessFile raf = new RandomAccessFile(tempDir + File.separator + segmentName, "rw")) {
      segment = new LogSegment(segmentName, file, capacityInBytes, alignment, metrics, writeHeader);
      verifyGetters(segment, segmentName, capacityInBytes, alignment);
    }
    return segment;
  }

  /**
   * Checks the permanent properties of the {@link LogSegment}.
   * @param segment the {@link LogSegment} to check.
   * @param segmentName the expected name of the segment.
   * @param capacityInBytes the expected capacity in bytes of the segment.
   * @param alignment the expected alignment of the segment.
   */
  private void verifyGetters(LogSegment segment, String segmentName, long capacityInBytes, long alignment) {
    assertEquals("Segment name not as expected", segmentName, segment.getName());
    assertEquals("Capacity not as expected", capacityInBytes, segment.getCapacityInBytes());
    assertEquals("Alignment not as expected", alignment, segment.getAlignment());
  }

  /**
   * Appends random data of size {@code size} to given {@code segment}.
   * @param segment the {@link LogSegment} to append data to.
   * @param size the size of the data that should be appended.
   * @return the data that was appended.
   * @throws IOException
   */
  private byte[] appendRandomData(LogSegment segment, int size) throws IOException {
    byte[] buf = TestUtils.getRandomBytes(size);
    segment.appendFrom(ByteBuffer.wrap(buf));
    return buf;
  }

  /**
   * Reads data starting from {@code offsetToStartRead} of {@code segment} and matches it with {@code original}.
   * @param segment the {@link LogSegment} to read from.
   * @param offsetToStartRead the offset in {@code segment} to start reading from.
   * @param original the byte array to compare against.
   * @throws IOException
   */
  private void readAndEnsureMatch(LogSegment segment, long offsetToStartRead, byte[] original) throws IOException {
    ByteBuffer readBuf = ByteBuffer.wrap(new byte[original.length]);
    segment.readInto(readBuf, offsetToStartRead);
    assertArrayEquals("Data read does not match data written", original, readBuf.array());
  }

  /**
   * Closes the {@code segment} and deletes the backing file.
   * @param segment the {@link LogSegment} that needs to be closed and whose backing file needs to be deleted.
   * @throws IOException
   */
  private void closeSegmentAndDeleteFile(LogSegment segment) throws IOException {
    segment.close();
    Pair<File, FileChannel> view = segment.getView();
    assertFalse("File channel is not closed", view.getSecond().isOpen());
    File segmentFile = view.getFirst();
    assertTrue("The segment file [" + segmentFile.getAbsolutePath() + "] could not be deleted", segmentFile.delete());
    segment.closeView();
  }

  /**
   * Asserts that the append operation fails because the write overflows.
   * @param segment the {@link LogSegment} to write to.
   * @param appender the {@link Appender} to use.
   * @param bufToWrite the {@link ByteBuffer} containing the data to write.
   * @throws IOException
   */
  private void assertAppendOverflow(LogSegment segment, Appender appender, ByteBuffer bufToWrite) throws IOException {
    long writeOverFlowCount = metrics.overflowWriteError.getCount();
    try {
      appender.append(segment, bufToWrite);
      fail("Append should have failed because data won't fit in the segment");
    } catch (IllegalArgumentException e) {
      assertEquals("Write overflow should have been reported", writeOverFlowCount + 1,
          metrics.overflowWriteError.getCount());
      assertEquals("Position of buffer has changed", 0, bufToWrite.position());
    }
  }

  /**
   * Gets the header from the {@code segment}.
   * @param segment the {@link LogSegment} to get the header for
   * @return the header from the {@code segment}.
   * @throws IOException
   */
  private byte[] getHeader(LogSegment segment) throws IOException {
    FileChannel channel = segment.getView().getSecond();
    ByteBuffer header = ByteBuffer.allocate(LogSegment.getCurrentVersionHeaderSize());
    channel.read(header, 0);
    return header.array();
  }

  // basicWriteAndReadTest() helpers

  /**
   * Tests appending and reading to make sure data is written and the data read is consistent with the data written.
   * @param segment the {@link LogSegment} to perform the tests on.
   * @param segmentName the expected name of {@code segment}.
   * @param expectedStartOffset the expected start offset of {@code segment}.
   * @param alignmentToUse the actual alignment value to use.
   * @throws IOException
   */
  private void doBasicWriteAndReadTest(LogSegment segment, String segmentName, long expectedStartOffset,
      int alignmentToUse) throws IOException {
    File segmentFile = segment.getView().getFirst();
    segment.closeView();
    assertEquals("Name of segment is inconsistent with what was provided", segmentName, segment.getName());
    assertEquals("Capacity of segment is inconsistent with what was provided", STANDARD_SEGMENT_SIZE,
        segment.getCapacityInBytes());
    assertEquals("Start offset is not as expected", expectedStartOffset, segment.getStartOffset());
    long expectedRemainingWritableCapacity =
        STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(expectedStartOffset, alignmentToUse);
    assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
        segment.getRemainingWritableCapacityInBytes());
    int writeSize = 100;
    byte[] buf = TestUtils.getRandomBytes(3 * writeSize);
    long firstWriteStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignmentToUse);
    // append with buffer
    int written = segment.appendFrom(ByteBuffer.wrap(buf, 0, writeSize));
    assertEquals("Size written did not match size of buffer provided", writeSize, written);
    assertEquals("End offset is not as expected", firstWriteStartOffset + writeSize, segment.getEndOffset());
    expectedRemainingWritableCapacity =
        STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignmentToUse);
    assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
        segment.getRemainingWritableCapacityInBytes());
    readAndEnsureMatch(segment, firstWriteStartOffset, Arrays.copyOfRange(buf, 0, writeSize));

    long secondWriteStartOffset = TestUtils.getAlignedOffset(firstWriteStartOffset + writeSize, alignmentToUse);
    // append with channel
    segment.appendFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, writeSize, writeSize))),
        writeSize);
    assertEquals("End offset is not as expected", secondWriteStartOffset + writeSize, segment.getEndOffset());
    expectedRemainingWritableCapacity =
        STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignmentToUse);
    assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
        segment.getRemainingWritableCapacityInBytes());
    readAndEnsureMatch(segment, secondWriteStartOffset, Arrays.copyOfRange(buf, writeSize, 2 * writeSize));

    long thirdWriteStartOffset = segment.getEndOffset();
    // use writeFrom (does not do any alignment)
    segment.writeFrom(Channels.newChannel(new ByteBufferInputStream(ByteBuffer.wrap(buf, 2 * writeSize, writeSize))),
        thirdWriteStartOffset, writeSize);
    assertEquals("End offset is not as expected", thirdWriteStartOffset + writeSize, segment.getEndOffset());
    expectedRemainingWritableCapacity =
        STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignmentToUse);
    assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
        segment.getRemainingWritableCapacityInBytes());
    readAndEnsureMatch(segment, thirdWriteStartOffset, Arrays.copyOfRange(buf, 2 * writeSize, buf.length));

    // check file size and end offset
    assertEquals("End offset is not equal to the cumulative bytes written", thirdWriteStartOffset + writeSize,
        segment.getEndOffset());
    expectedRemainingWritableCapacity =
        STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignmentToUse);
    assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
        segment.getRemainingWritableCapacityInBytes());
    assertEquals("Size in bytes is not equal to size written", thirdWriteStartOffset + writeSize,
        segment.sizeInBytes());
    assertEquals("Capacity is not equal to allocated size ", STANDARD_SEGMENT_SIZE, segment.getCapacityInBytes());

    // ensure flush doesn't throw any errors.
    segment.flush();
    // close and reopen segment and ensure persistence.
    segment.close();
    if (writeHeader) {
      segment = new LogSegment(segmentName, segmentFile, metrics);
    } else {
      segment = new LogSegment(segmentName, segmentFile, STANDARD_SEGMENT_SIZE, alignmentToUse, metrics, false);
    }
    verifyGetters(segment, segmentName, STANDARD_SEGMENT_SIZE, alignmentToUse);
    segment.setEndOffset(thirdWriteStartOffset + writeSize);
    readAndEnsureMatch(segment, firstWriteStartOffset, Arrays.copyOfRange(buf, 0, writeSize));
    readAndEnsureMatch(segment, secondWriteStartOffset, Arrays.copyOfRange(buf, writeSize, 2 * writeSize));
    readAndEnsureMatch(segment, thirdWriteStartOffset, Arrays.copyOfRange(buf, 2 * writeSize, buf.length));
  }

  // viewAndRefCountTest() helpers

  /**
   * Gets a view of the given {@code segment} and verifies the ref count and the data obtained from the view against
   * {@code expectedRefCount} and {@code dataInSegment} respectively.
   * @param segment the {@link LogSegment} to get a view from.
   * @param writeStartOffset the offset at which write was started on the segment.
   * @param offset the offset for which a view is required.
   * @param dataInSegment the entire data in the {@link LogSegment}.
   * @param expectedRefCount the expected return value of {@link LogSegment#refCount()} once the view is obtained from
   *                         the {@code segment}
   * @throws IOException
   */
  private void getAndVerifyView(LogSegment segment, long writeStartOffset, int offset, byte[] dataInSegment,
      long expectedRefCount) throws IOException {
    Random random = new Random();
    Pair<File, FileChannel> view = segment.getView();
    assertNotNull("File object received in view is null", view.getFirst());
    assertNotNull("FileChannel object received in view is null", view.getSecond());
    assertEquals("Ref count is not as expected", expectedRefCount, segment.refCount());
    int sizeToRead = random.nextInt(dataInSegment.length - offset + 1);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[sizeToRead]);
    view.getSecond().read(buffer, writeStartOffset + offset);
    assertArrayEquals("Data read from file does not match data written",
        Arrays.copyOfRange(dataInSegment, offset, offset + sizeToRead), buffer.array());
  }

  // appendTest() helpers

  /**
   * Using the given {@code appender}'s {@link Appender#append(LogSegment, ByteBuffer)} function, tests for various
   * cases for append operations.
   * @param appender the {@link Appender} to use
   * @throws IOException
   */
  private void doAppendTest(Appender appender) throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long firstWriteStartOffset = TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      byte[] bufOne = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 2);
      byte[] bufTwo = TestUtils.getRandomBytes(STANDARD_SEGMENT_SIZE / 3);

      appender.append(segment, ByteBuffer.wrap(bufOne));
      assertEquals("End offset is not as expected", firstWriteStartOffset + bufOne.length, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());

      long secondWriteStartOffset = TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      appender.append(segment, ByteBuffer.wrap(bufTwo));
      assertEquals("End offset is not as expected", secondWriteStartOffset + bufTwo.length, segment.getEndOffset());
      expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());

      // try to do a write that won't fit
      long thirdWriteStartOffset = TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      ByteBuffer failBuf =
          ByteBuffer.wrap(TestUtils.getRandomBytes((int) (STANDARD_SEGMENT_SIZE - thirdWriteStartOffset + 1)));
      assertAppendOverflow(segment, appender, failBuf);

      // read and ensure data matches
      readAndEnsureMatch(segment, firstWriteStartOffset, bufOne);
      readAndEnsureMatch(segment, secondWriteStartOffset, bufTwo);

      segment.close();
      // ensure that append fails.
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1));
      try {
        appender.append(segment, buffer);
        fail("Append should have failed because segments are closed");
      } catch (ClosedChannelException e) {
        assertEquals("Position of buffer has changed", 0, buffer.position());
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  // alignedWritesTest() helpers

  /**
   * Performs aligned writes with the provided {@code appender}.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void doAlignedWritesTest(Appender appender) throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      int leftOverSpace = (int) (STANDARD_SEGMENT_SIZE - writeStartOffset);
      int bufSize = (5 + 4 + 3 + 2 + 1) * alignment;
      assertTrue("This test needs some preconditions to be met for it to run", bufSize < leftOverSpace);
      byte[] buf = TestUtils.getRandomBytes(bufSize);
      int position = 0;
      for (int writeSize = alignment; writeSize <= 5 * alignment; writeSize += alignment) {
        appender.append(segment, ByteBuffer.wrap(buf, position, writeSize));
        readAndEnsureMatch(segment, writeStartOffset + position,
            Arrays.copyOfRange(buf, position, position + writeSize));
        position += writeSize;
      }
      // since all writes are aligned, there should be no "holes"
      readAndEnsureMatch(segment, writeStartOffset, buf);
      assertEquals("End offset not as expected", writeStartOffset + bufSize, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  // nonAlignedWritesTest() helpers

  /**
   * Performs non-aligned writes with the provided {@code appender}.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void doNonAlignedWritesTest(Appender appender) throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      int leftOverSpace = (int) (STANDARD_SEGMENT_SIZE - writeStartOffset);
      int requiredSpace = (5 + 4 + 3 + 2 + 1) * alignment;
      assertTrue("This test needs some preconditions to be met for it to run", requiredSpace < leftOverSpace);
      // the expected end offset accounts for all alignments except the last one.
      long expectedEndOffset = writeStartOffset + requiredSpace - 1;
      // each time a write that is 1 byte less than the alignment is made
      byte[] buf = TestUtils.getRandomBytes(requiredSpace - 5);
      int position = 0;
      for (int writeSize = alignment - 1; writeSize <= 5 * alignment - 1; writeSize += alignment) {
        appender.append(segment, ByteBuffer.wrap(buf, position, writeSize));
        writeStartOffset = TestUtils.getAlignedOffset(writeStartOffset, alignment);
        readAndEnsureMatch(segment, writeStartOffset, Arrays.copyOfRange(buf, position, position + writeSize));
        position += writeSize;
        writeStartOffset += writeSize;
      }
      assertEquals("End offset not as expected", expectedEndOffset, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  // changingAlignmentsTest() helpers

  /**
   * Does the test where the alignment provided to log segments can change across reloads.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void doChangingAlignmentsTest(Appender appender) throws IOException {
    String segmentName = "log_current";
    LogSegment segment = getSegment(segmentName, STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      File segmentFile = segment.getView().getFirst();
      segment.closeView();
      segment.close();
      byte[] buf = TestUtils.getRandomBytes(11);
      int numWrites = 5;
      long savedEndOffset = segment.getStartOffset();
      for (int i = 0; i < numWrites; i++) {
        long expectedEndOffset;
        long expectedRemainingWritableCapacity;
        if (writeHeader) {
          segment = new LogSegment(segmentName, segmentFile, metrics);
          verifyGetters(segment, segmentName, STANDARD_SEGMENT_SIZE, alignment);
          segment.setEndOffset(savedEndOffset);
          // alignment does not change across reloads
          long writeStartOffset = TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
          expectedEndOffset = writeStartOffset + buf.length;
          expectedRemainingWritableCapacity =
              STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(expectedEndOffset, alignment);
          appender.append(segment, ByteBuffer.wrap(buf));
          readAndEnsureMatch(segment, writeStartOffset, buf);
        } else {
          int alignmentToSet = alignment + i;
          segment = new LogSegment(segmentName, segmentFile, STANDARD_SEGMENT_SIZE, alignmentToSet, metrics, false);
          verifyGetters(segment, segmentName, STANDARD_SEGMENT_SIZE, alignmentToSet);
          segment.setEndOffset(savedEndOffset);
          // alignment changes across reloads
          long writeStartOffset = TestUtils.getAlignedOffset(segment.getEndOffset(), alignmentToSet);
          expectedEndOffset = writeStartOffset + buf.length;
          expectedRemainingWritableCapacity =
              STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(expectedEndOffset, alignmentToSet);
          appender.append(segment, ByteBuffer.wrap(buf));
          readAndEnsureMatch(segment, writeStartOffset, buf);
        }
        assertEquals("End offset not as expected", expectedEndOffset, segment.getEndOffset());
        assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
            segment.getRemainingWritableCapacityInBytes());
        savedEndOffset = segment.getEndOffset();
        segment.close();
      }
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  // notEnoughAlignedSpaceTest() helpers

  /**
   * Tests for append failure when there is enough non-aligned space but not enough aligned space.
   * @param appender the {@link Appender} to use.
   * @throws IOException
   */
  private void doNotEnoughAlignedSpaceTest(Appender appender) throws IOException {
    LogSegment segment = getSegment("log_current", STANDARD_SEGMENT_SIZE, alignment, writeHeader);
    try {
      // write data such that there is alignment - 1 left over space
      long writeStartOffset = TestUtils.getAlignedOffset(segment.getStartOffset(), alignment);
      int toWrite = (int) (STANDARD_SEGMENT_SIZE - writeStartOffset - alignment + 1);
      long expectedEndOffset = writeStartOffset + toWrite;
      appender.append(segment, ByteBuffer.allocate(toWrite));
      assertEquals("End offset not as expected", expectedEndOffset, segment.getEndOffset());
      long expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
      writeStartOffset = TestUtils.getAlignedOffset(expectedEndOffset, alignment);
      toWrite = Math.max(1, (int) (STANDARD_SEGMENT_SIZE - writeStartOffset + 1));
      // append should fail because there is enough aligned space but not enough non-aligned space
      assertAppendOverflow(segment, appender, ByteBuffer.allocate(toWrite));
      assertEquals("End offset not as expected", expectedEndOffset, segment.getEndOffset());
      expectedRemainingWritableCapacity =
          STANDARD_SEGMENT_SIZE - TestUtils.getAlignedOffset(segment.getEndOffset(), alignment);
      assertEquals("Remaining writable capacity not as expected", expectedRemainingWritableCapacity,
          segment.getRemainingWritableCapacityInBytes());
    } finally {
      closeSegmentAndDeleteFile(segment);
    }
  }

  /**
   * Interface for abstracting append operations.
   */
  private interface Appender {
    /**
     * Appends the data of {@code buffer} to {@code segment}.
     * @param segment the {@link LogSegment} to append {@code buffer} to.
     * @param buffer the data to append to {@code segment}.
     * @throws IOException
     */
    void append(LogSegment segment, ByteBuffer buffer) throws IOException;
  }
}

