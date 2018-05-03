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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class JournalTest {

  @Test
  public void testJournalOperation() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String firstLogSegmentName = LogSegmentNameHelper.getName(pos, gen);
    String secondLogSegmentName = LogSegmentNameHelper.getNextPositionName(firstLogSegmentName);
    Journal journal = new Journal("test", 10, 5);
    Assert.assertNull("First offset should be null", journal.getFirstOffset());
    Assert.assertNull("Last offset should be null", journal.getLastOffset());
    Assert.assertNull("Should not be able to get entries because there are none",
        journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true));
    Assert.assertFalse("Journal should not be full", journal.isFull());
    Offset firstOffset = new Offset(firstLogSegmentName, 0);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 0), new MockId("id1"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 1000), new MockId("id2"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 2000), new MockId("id3"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 3000), new MockId("id4"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(firstLogSegmentName, 4000), new MockId("id5"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 0), new MockId("id6"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 1000), new MockId("id7"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 2000), new MockId("id8"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 3000), new MockId("id9"), firstOffset, false);
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 4000), new MockId("id10"), firstOffset, true);
    List<JournalEntry> entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 0));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id1"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(firstLogSegmentName, 4000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id5"));
    entries = journal.getEntriesSince(new Offset(secondLogSegmentName, 0), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(secondLogSegmentName, 1000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id7"));
    Assert.assertEquals(entries.get(3).getOffset(), new Offset(secondLogSegmentName, 4000));
    Assert.assertEquals(entries.get(3).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 4);
    entries = journal.getEntriesSince(new Offset(secondLogSegmentName, 2000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(secondLogSegmentName, 3000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id9"));
    Assert.assertEquals(entries.get(1).getOffset(), new Offset(secondLogSegmentName, 4000));
    Assert.assertEquals(entries.get(1).getKey(), new MockId("id10"));
    Assert.assertEquals(entries.size(), 2);
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 2000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 3000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id4"));
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(secondLogSegmentName, 2000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id8"));
    Assert.assertEquals(entries.size(), 5);
    for (boolean inclusive : new boolean[]{true, false}) {
      entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 1), inclusive);
      Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 1000));
      Assert.assertEquals(entries.get(0).getKey(), new MockId("id2"));
      Assert.assertEquals(entries.size(), 5);
      Assert.assertEquals(entries.get(4).getOffset(), new Offset(secondLogSegmentName, 0));
      Assert.assertEquals(entries.get(4).getKey(), new MockId("id6"));
    }
    addEntryAndVerify(journal, new Offset(secondLogSegmentName, 5000), new MockId("id11"),
        new Offset(firstLogSegmentName, 1000), true);
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 0), true);
    Assert.assertNull(entries);
    entries = journal.getEntriesSince(new Offset(firstLogSegmentName, 1000), false);
    Assert.assertEquals(entries.get(0).getOffset(), new Offset(firstLogSegmentName, 2000));
    Assert.assertEquals(entries.get(0).getKey(), new MockId("id3"));
    Assert.assertEquals(entries.size(), 5);
    Assert.assertEquals(entries.get(4).getOffset(), new Offset(secondLogSegmentName, 1000));
    Assert.assertEquals(entries.get(4).getKey(), new MockId("id7"));
  }

  /**
   * Adds an entry to the {@code journal} and verifies values of some getters
   * @param journal the {@link Journal} instance to add the entry to
   * @param offsetToAdd the {@link Offset} to add
   * @param key the {@link StoreKey} to add
   * @param firstOffset the expected value of {@link Journal#getFirstOffset()}
   * @param isFull the expected value of {@link Journal#isFull()}
   */
  private void addEntryAndVerify(Journal journal, Offset offsetToAdd, StoreKey key, Offset firstOffset,
      boolean isFull) {
    journal.addEntry(offsetToAdd, key);
    Assert.assertEquals("First offset not as expected", firstOffset, journal.getFirstOffset());
    Assert.assertEquals("Last offset not as expected", offsetToAdd, journal.getLastOffset());
    Assert.assertEquals("Value of isFull not as expected", isFull, journal.isFull());
  }
}
