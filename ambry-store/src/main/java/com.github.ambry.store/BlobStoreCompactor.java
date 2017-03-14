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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Component that removes the "dead" data from the set of provided log segments and reclaims space.
 * <p/>
 * It does this by copying the valid data into swap spaces and atomically replacing the segments being compacted with
 * the swap spaces.
 */
class BlobStoreCompactor {
  static final String INDEX_SEGMENT_READ_JOB_NAME = "blob_store_compactor_index_segment_read";
  static final String LOG_SEGMENT_COPY_JOB_NAME = "blob_store_compactor_log_segment_copy";
  static final String TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME = "compactor_clean_shutdown";
  static final FilenameFilter TEMP_LOG_SEGMENTS_FILTER = new FilenameFilter() {
    private final String SUFFIX = LogSegmentNameHelper.SUFFIX + TEMP_LOG_SEGMENT_NAME_SUFFIX;

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(SUFFIX);
    }
  };

  private static final long WAIT_TIME_FOR_CLEANUP_MS = 5 * Time.MsPerSec;
  private static final String TEMP_LOG_SEGMENT_NAME_SUFFIX = BlobStore.SEPARATOR + "temp";

  private final File dataDir;
  private final String storeId;
  private final StoreKeyFactory storeKeyFactory;
  private final StoreConfig config;
  private final StoreMetrics metrics;
  private final Log srcLog;
  private final DiskIOScheduler diskIOScheduler;
  private final ScheduledExecutorService scheduler;
  private final MessageStoreRecovery recovery;
  private final Time time;
  private final UUID sessionId;
  private final UUID incarnationId;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private volatile boolean isActive = false;
  private PersistentIndex srcIndex;

  private Log tgtLog;
  private PersistentIndex tgtIndex;
  private long numSwapsUsed;
  private StoreFindToken recoveryStartToken = null;
  private CompactionLog compactionLog;
  private volatile CountDownLatch runningLatch = new CountDownLatch(0);

  /**
   * Constructs the compactor component.
   * @param dataDir the directory where all compactions need to be run.
   * @param storeId the unique ID of the store.
   * @param storeKeyFactory the {@link StoreKeyFactory} to generate {@link StoreKey} instances.
   * @param config the {@link StoreConfig} that defines configuration parameters.
   * @param metrics the {@link StoreMetrics} to use to record metrics.
   * @param diskIOScheduler the {@link DiskIOScheduler} to schedule I/O.
   * @param srcLog the {@link Log} to copy data from.
   * @param scheduler the {@link ScheduledExecutorService} to use.
   * @param recovery the {@link MessageStoreRecovery} to use to recover the index and log.
   * @param time the {@link Time} instance to use.
   * @param sessionId the sessionID of the store.
   * @param incarnationId the incarnation ID of the store.
   */
  BlobStoreCompactor(String dataDir, String storeId, StoreKeyFactory storeKeyFactory, StoreConfig config,
      StoreMetrics metrics, DiskIOScheduler diskIOScheduler, Log srcLog, ScheduledExecutorService scheduler,
      MessageStoreRecovery recovery, Time time, UUID sessionId, UUID incarnationId) {
    this.dataDir = new File(dataDir);
    this.storeId = storeId;
    this.storeKeyFactory = storeKeyFactory;
    this.config = config;
    this.metrics = metrics;
    this.srcLog = srcLog;
    this.diskIOScheduler = diskIOScheduler;
    this.scheduler = scheduler;
    this.recovery = recovery;
    this.time = time;
    this.sessionId = sessionId;
    this.incarnationId = incarnationId;
  }

  /**
   * If a compaction was in progress during a crash/shutdown, fixes the state so that the store is loaded correctly
   * and compaction can resume smoothly on a call to {@link #resumeCompaction()}. Expected to be called before the
   * {@link PersistentIndex} is created in the {@link BlobStore}.
   * @throws IOException if the {@link CompactionLog} could not be created or if commit or cleanup failed.
   * @throws StoreException if the commit failed.
   */
  void fixStateIfRequired() throws IOException, StoreException {
    if (CompactionLog.isCompactionInProgress(dataDir.getAbsolutePath(), storeId)) {
      compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, storeKeyFactory, time);
      CompactionLog.Phase phase = compactionLog.getCompactionPhase();
      switch (phase) {
        case COMMIT:
          commit(true);
          compactionLog.markCleanupStart();
        case CLEANUP:
          cleanup(true);
          compactionLog.markCycleComplete();
          break;
        case COPY:
          recoveryStartToken = compactionLog.getSafeToken();
        case PREPARE:
        case DONE:
          break;
        default:
          throw new IllegalStateException("Unrecognized compaction phase: " + phase);
      }
      if (compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
        endCompaction();
      }
    }
  }

  /**
   * Starts the compactor and sets the {@link PersistentIndex} to copy data from.
   * @param srcIndex the {@link PersistentIndex} to copy data from.
   */
  void start(PersistentIndex srcIndex) {
    this.srcIndex = srcIndex;
    if (compactionLog == null && srcIndex.hardDeleter != null && srcIndex.hardDeleter.isPaused()) {
      srcIndex.hardDeleter.resume();
    }
    isActive = true;
  }

  /**
   * Shuts down the compactor and waits for {@code waitTimeSecs} for the shutdown to complete.
   * @param waitTimeSecs the number of seconds to wait for shutdown to complete.
   * @throws InterruptedException if the wait for shutdown was interrupted.
   */
  void shutdown(long waitTimeSecs) throws InterruptedException {
    isActive = false;
    if (waitTimeSecs > 0 && !runningLatch.await(waitTimeSecs, TimeUnit.SECONDS)) {
      logger.error("Compactor did not shutdown within 2 seconds");
    }
  }

  /**
   * Compacts the store by copying valid data from the log segments in {@code details} to swap spaces and switching
   * the compacted segments out for the swap spaces. Returns once the compaction is complete.
   * @param details the {@link CompactionDetails} for the compaction.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log, if one or more offsets
   * in the segments to compact are in the journal or if the last modified time of any of the provided log segments is
   * greater than the provided reference time.
   * @throws IllegalStateException if the compactor has not been started.
   * @throws InterruptedException if the compaction was interrupted
   * @throws IOException if there were I/O errors during copying, commit or cleanup.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  void compact(CompactionDetails details) throws InterruptedException, IOException, StoreException {
    if (srcIndex == null) {
      throw new IllegalStateException("Compactor has not been started");
    }
    checkSanity(details);
    compactionLog = new CompactionLog(dataDir.getAbsolutePath(), storeId, time, details);
    resumeCompaction();
  }

  /**
   * Resumes compaction from where it was left off.
   * @throws IllegalStateException if the compactor has not been started or if there is no compaction to resume.
   * @throws InterruptedException if the compaction was interrupted
   * @throws IOException if there were I/O errors during copying, commit or cleanup.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  void resumeCompaction() throws InterruptedException, IOException, StoreException {
    if (srcIndex == null) {
      throw new IllegalStateException("Compactor has not been started");
    } else if (compactionLog == null) {
      throw new IllegalStateException("There is no compaction to resume");
    }
    runningLatch = new CountDownLatch(1);
    while (isActive && !compactionLog.getCompactionPhase().equals(CompactionLog.Phase.DONE)) {
      CompactionLog.Phase phase = compactionLog.getCompactionPhase();
      switch (phase) {
        case PREPARE:
          compactionLog.markCopyStart();
        case COPY:
          copy();
          if (isActive) {
            compactionLog.markCommitStart();
            commit(false);
            compactionLog.markCleanupStart();
            waitTillRefCountZero();
            cleanup(false);
            compactionLog.markCycleComplete();
          }
          break;
        default:
          throw new IllegalStateException("Illegal compaction phase: " + phase);
      }
    }
    endCompaction();
    runningLatch.countDown();
  }

  /**
   * Checks the sanity of the provided {@code details}
   * @param details the {@link CompactionDetails} to use for compaction.
   * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log, if one or more offsets
   * in the segments to compact are in the journal or if the last modified time of any of the provided log segments is
   * greater than the provided reference time.
   */
  private void checkSanity(CompactionDetails details) {
    List<String> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    LogSegment lastSegment = null;
    // all segments should be available
    for (String segmentName : segmentsUnderCompaction) {
      lastSegment = srcLog.getSegment(segmentName);
      if (lastSegment == null) {
        throw new IllegalArgumentException(segmentName + " does not exist in the log");
      }
    }
    // last offset to compact should be outside the range of the journal
    Offset lastSegmentEndOffset = new Offset(lastSegment.getName(), lastSegment.getEndOffset());
    if (lastSegmentEndOffset.compareTo(srcIndex.journal.getFirstOffset()) >= 0) {
      throw new IllegalArgumentException("Some of the offsets provided for compaction are within the jounral");
    }
  }

  /**
   * Copies valid data from the source log segments into the swap spaces.
   * <p/>
   * Splits the compaction into two cycles if there aren't enough swap spaces and completes the copy for the current
   * cycle.
   * @throws InterruptedException if the compaction was interrupted
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void copy() throws InterruptedException, IOException, StoreException {
    setupState();
    List<String> logSegmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
    Offset cutoffOffsetForDelete = getCutoffOffsetForDelete();
    FileSpan duplicateSearchSpan = null;
    if (compactionLog.getCurrentIdx() > 0) {
      LogSegment logSegment = srcLog.getSegment(logSegmentsUnderCompaction.get(0));
      LogSegment prevSegment = srcLog.getPrevSegment(logSegment);
      if (prevSegment != null) {
        // duplicate data, if it exists, can only be in the previous log segment.
        Offset startOffset = new Offset(prevSegment.getName(), prevSegment.getStartOffset());
        Offset endOffset = new Offset(prevSegment.getName(), prevSegment.getEndOffset());
        duplicateSearchSpan = new FileSpan(startOffset, endOffset);
      }
    }

    for (String logSegmentName : logSegmentsUnderCompaction) {
      LogSegment srcLogSegment = srcLog.getSegment(logSegmentName);
      Offset logSegmentEndOffset = new Offset(srcLogSegment.getName(), srcLogSegment.getEndOffset());
      if (needsCopying(logSegmentEndOffset) && !copyLogSegmentData(srcLogSegment, duplicateSearchSpan,
          cutoffOffsetForDelete)) {
        if (isActive) {
          compactionLog.splitCurrentCycle(logSegmentName);
        }
        // else shutdown is in progress.
        break;
      }
      // only the very first log segment in the cycle can contain duplicate records.
      duplicateSearchSpan = null;
    }

    numSwapsUsed = tgtIndex.getLogSegmentCount();
    tgtIndex.close();
    tgtLog.close();
    // persist the bloom of the "latest" index segment if it exists
    if (numSwapsUsed > 0) {
      tgtIndex.getIndexSegments().lastEntry().getValue().map(true);
    } else {
      // there were no valid entries copied, return any temp segments back to the pool
      File[] files = dataDir.listFiles(TEMP_LOG_SEGMENTS_FILTER);
      if (files != null) {
        for (File file : files) {
          // TODO (DiskManager changes): This will actually return the segment to the DiskManager pool.
          if (!file.delete()) {
            throw new IllegalStateException("Could not delete segment file: " + file.getAbsolutePath());
          }
        }
      } else {
        throw new StoreException("Could not list temp files in directory: " + dataDir, StoreErrorCodes.Unknown_Error);
      }
    }
  }

  /**
   * Commits the changes made by copying valid data. Commit involves
   * 1. Renaming temporary log segments
   * 2. Adding them to the log segments maintained by the application log.
   * 3. Atomically switching the old set of index segments for the new ones.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were I/O errors during committing.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void commit(boolean recovering) throws IOException, StoreException {
    List<String> logSegmentNames = getTargetLogSegmentNames();
    renameLogSegments(logSegmentNames);
    commitLogSegments(logSegmentNames, recovering);
    if (!recovering) {
      commitIndexSegments(logSegmentNames);
      if (srcIndex.hardDeleter != null) {
        srcIndex.hardDeleter.resume();
      }
    }
  }

  /**
   * Cleans up after the commit is complete. Cleaning up involves:
   * 1. Dropping the log segments that are no longer relevant.
   * 2. Deleting all associated index segment and bloom files.
   * 3. Deleting the clean shutdown file associated with the index of the swap spaces.
   * 4. Resetting state.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were I/O errors during cleanup.
   */
  private void cleanup(boolean recovering) throws IOException {
    cleanupLogAndIndexSegments(recovering);
    File cleanShutdownFile = new File(dataDir, TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME);
    if (cleanShutdownFile.exists() && !cleanShutdownFile.delete()) {
      logger.warn("Could not delete the clean shutdown file {}", cleanShutdownFile);
    }
    resetStructures();
  }

  // copy() helpers

  /**
   * Sets up the state required for copying data by populating all the data structures, creating a log and index that
   * wraps the swap spaces and pausing hard delete in the application log.
   * @throws InterruptedException if hard delete could not be paused.
   * @throws IOException if logs and indexes could not be set up.
   * @throws StoreException if there were exceptions reading to writing to store components.
   */
  private void setupState() throws InterruptedException, IOException, StoreException {
    CompactionDetails details = compactionLog.getCompactionDetails();
    long highestGeneration = 0;
    for (String segmentName : details.getLogSegmentsUnderCompaction()) {
      highestGeneration = Math.max(highestGeneration, LogSegmentNameHelper.getGeneration(segmentName));
    }
    List<Pair<String, String>> targetSegmentNamesAndFilenames = new ArrayList<>();
    List<LogSegment> existingTargetLogSegments = new ArrayList<>();
    for (String segmentName : details.getLogSegmentsUnderCompaction()) {
      long pos = LogSegmentNameHelper.getPosition(segmentName);
      String targetSegmentName = LogSegmentNameHelper.getName(pos, highestGeneration + 1);
      String targetSegmentFileName =
          LogSegmentNameHelper.nameToFilename(targetSegmentName) + TEMP_LOG_SEGMENT_NAME_SUFFIX;
      File targetSegmentFile = new File(dataDir, targetSegmentFileName);
      if (targetSegmentFile.exists()) {
        existingTargetLogSegments.add(new LogSegment(targetSegmentName, targetSegmentFile, metrics));
      } else {
        targetSegmentNamesAndFilenames.add(new Pair<>(targetSegmentName, targetSegmentFileName));
      }
    }
    // TODO: available swap space count should be obtained from DiskManager. For now, assumed to be 1.
    long targetLogTotalCapacity = srcLog.getSegmentCapacity();
    tgtLog = new Log(dataDir.getAbsolutePath(), targetLogTotalCapacity, srcLog.getSegmentCapacity(), metrics, true,
        existingTargetLogSegments, targetSegmentNamesAndFilenames.iterator());
    Journal journal = new Journal(dataDir.getAbsolutePath(), 2 * config.storeIndexMaxNumberOfInmemElements,
        config.storeMaxNumberOfEntriesToReturnFromJournal);
    tgtIndex =
        new PersistentIndex(dataDir.getAbsolutePath(), scheduler, tgtLog, config, storeKeyFactory, recovery, null,
            metrics, journal, time, sessionId, incarnationId, TARGET_INDEX_CLEAN_SHUTDOWN_FILE_NAME);
    if (srcIndex.hardDeleter != null && !srcIndex.hardDeleter.isPaused()) {
      srcIndex.hardDeleter.pause();
    }
  }

  /**
   * Determines if the offset has been copied or requires copying.
   * @param offset the {@link Offset} to test at.
   * @return {@code true} if copying is required. {@code false} otherwise.
   */
  private boolean needsCopying(Offset offset) {
    return recoveryStartToken == null || recoveryStartToken.getOffset().compareTo(offset) < 0;
  }

  /**
   * @return the {@link Offset} up until which delete records are considered applicable. Any delete records of blobs
   * after this offset does not count and the blob is considered as not deleted.
   */
  private Offset getCutoffOffsetForDelete() {
    // TODO: move this to BlobStoreStats
    long referenceTimeMs = compactionLog.getCompactionDetails().getReferenceTimeMs();
    Offset cutoffOffset = srcIndex.getStartOffset();
    for (IndexSegment indexSegment : srcIndex.getIndexSegments().descendingMap().values()) {
      if (indexSegment.getLastModifiedTimeMs() < referenceTimeMs) {
        cutoffOffset = indexSegment.getEndOffset();
        break;
      }
    }
    return cutoffOffset;
  }

  /**
   * Copies data from the provided log segment into the target log (swap spaces).
   * @param from the {@link LogSegment} to copy from.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates.
   * @param cutoffOffsetForDelete the {@link Offset} up until which delete records are considered applicable.
   * @return {@code true} if all the records in the log segment were copied. {@code false} if some records were not
   * copied either because there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyLogSegmentData(LogSegment from, FileSpan duplicateSearchSpan, Offset cutoffOffsetForDelete)
      throws IOException, StoreException {
    boolean allCopied = true;
    for (Offset indexSegmentStartOffset : getIndexSegmentDetails(from.getName()).keySet()) {
      IndexSegment toCopy = srcIndex.getIndexSegments().get(indexSegmentStartOffset);
      if (needsCopying(toCopy.getEndOffset()) && !copyIndexSegmentData(from, toCopy, duplicateSearchSpan,
          cutoffOffsetForDelete)) {
        // there is a shutdown in progress or there was no space to copy all entries.
        allCopied = false;
        break;
      }
    }
    return allCopied;
  }

  /**
   * Copies data in the provided {@code indexSegmentToCopy} into the target log (swap spaces).
   * @param from the {@link LogSegment} to copy from.
   * @param indexSegmentToCopy the {@link IndexSegment} that contains the entries that need to be copied.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates.
   * @param cutoffOffsetForDelete the {@link Offset} up until which delete records are considered applicable.
   * @return {@code true} if all the records in the index segment were copied. {@code false} if some records were not
   * copied either because there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyIndexSegmentData(LogSegment from, IndexSegment indexSegmentToCopy, FileSpan duplicateSearchSpan,
      Offset cutoffOffsetForDelete) throws IOException, StoreException {
    List<IndexEntry> allIndexEntries = new ArrayList<>();
    // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
    diskIOScheduler.getSlice(INDEX_SEGMENT_READ_JOB_NAME, INDEX_SEGMENT_READ_JOB_NAME, 1);
    // get all entries
    indexSegmentToCopy.getIndexEntriesSince(null, new FindEntriesCondition(Long.MAX_VALUE), allIndexEntries,
        new AtomicLong(0));

    // save a token for restart (the key gets ignored but is required to be non null for construction)
    StoreFindToken safeToken =
        new StoreFindToken(allIndexEntries.get(0).getKey(), indexSegmentToCopy.getStartOffset(), sessionId,
            incarnationId);
    compactionLog.setSafeToken(safeToken);

    boolean checkAlreadyCopied =
        recoveryStartToken != null && recoveryStartToken.getOffset().equals(indexSegmentToCopy.getStartOffset());

    // filter deleted/expired entries and get index entries for all the PUT and DELETE records
    List<IndexEntry> indexEntriesToCopy =
        getIndexEntriesToCopy(allIndexEntries, duplicateSearchSpan, indexSegmentToCopy.getStartOffset(),
            cutoffOffsetForDelete, checkAlreadyCopied);

    // Copy these over
    boolean copiedAll = copyRecords(from, indexEntriesToCopy, indexSegmentToCopy.getLastModifiedTimeSecs());
    // persist
    tgtIndex.persistIndex();
    return copiedAll;
  }

  /**
   * Gets all the index entries that need to be copied. The index entries that will be copied are:
   * 1. Records that are not duplicates.
   * 2. Records that are not expired or deleted.
   * 3. Delete records.
   * 4. Records that are already in the target log (because they were copied before crash/shutdown).
   * @param allIndexEntries the {@link MessageInfo} of all the entries in the index segment.
   * @param duplicateSearchSpan the {@link FileSpan} in which to search for duplicates
   * @param indexSegmentStartOffset the start {@link Offset} of the index segment under consideration.
   * @param cutoffOffsetForDelete the {@link Offset} up until which delete records are considered applicable.
   * @param checkAlreadyCopied {@code true} if a check for existence in the swap spaces has to be executed (due to
   *                                       crash/shutdown), {@code false} otherwise.
   * @return the list of valid {@link IndexEntry} sorted by their offset.
   * @throws IllegalStateException if there is a mismatch b/w the index value expected and the index value obtained.
   * @throws StoreException if there is any problem with using the index.
   */
  private List<IndexEntry> getIndexEntriesToCopy(List<IndexEntry> allIndexEntries, FileSpan duplicateSearchSpan,
      Offset indexSegmentStartOffset, Offset cutoffOffsetForDelete, boolean checkAlreadyCopied) throws StoreException {
    List<IndexEntry> indexEntriesToCopy = new ArrayList<>();
    List<IndexEntry> copyCandidates =
        getValidIndexEntries(indexSegmentStartOffset, allIndexEntries, cutoffOffsetForDelete);
    for (IndexEntry copyCandidate : copyCandidates) {
      IndexValue copyCandidateValue = copyCandidate.getValue();
      // search for duplicates in srcIndex if required
      if (duplicateSearchSpan != null) {
        IndexValue possibleDuplicate = srcIndex.findKey(copyCandidate.getKey(), duplicateSearchSpan);
        if (possibleDuplicate != null) {
          if (possibleDuplicate.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // copyCandidate is surely a duplicate because srcIndex contains a DELETE index entry
            continue;
          } else if (!copyCandidateValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // copyCandidate is a duplicate because it is a PUT entry and so is the possible duplicate.
            continue;
          }
        }
      }
      // search for duplicates in tgtIndex if required
      if (!checkAlreadyCopied || !alreadyExistsInTgt(copyCandidate.getKey(), copyCandidateValue)) {
        logger.trace("Adding index entry for {} because it is valid and does not already exist",
            copyCandidate.getKey());
        indexEntriesToCopy.add(copyCandidate);
      }
    }
    // order by offset in log.
    Collections.sort(indexEntriesToCopy, PersistentIndex.INDEX_ENTRIES_OFFSET_COMPARATOR);
    return indexEntriesToCopy;
  }

  private List<IndexEntry> getValidIndexEntries(Offset indexSegmentStartOffset, List<IndexEntry> allIndexEntries,
      Offset cutoffOffsetForDelete) throws StoreException {
    // TODO: move this blob store stats
    boolean deletesInEffect = indexSegmentStartOffset.compareTo(cutoffOffsetForDelete) < 0;
    List<IndexEntry> validEntries = new ArrayList<>();
    for (IndexEntry indexEntry : allIndexEntries) {
      IndexValue value = indexEntry.getValue();
      if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
        // DELETE entry. Always valid.
        validEntries.add(indexEntry);
        // if this delete cannot be counted and there is a corresponding unexpired PUT entry in the same index segment,
        // we will need to add it.
        if (!deletesInEffect && !srcIndex.isExpired(value)) {
          long putRecordOffset = value.getOriginalMessageOffset();
          if (putRecordOffset != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET
              && indexSegmentStartOffset.getOffset() <= putRecordOffset) {
            BlobReadOptions options =
                srcIndex.getBlobReadInfo(indexEntry.getKey(), EnumSet.allOf(StoreGetOptions.class));
            Offset offset = new Offset(indexSegmentStartOffset.getName(), putRecordOffset);
            IndexValue putValue = new IndexValue(options.getSize(), offset, value.getExpiresAtMs());
            validEntries.add(new IndexEntry(indexEntry.getKey(), putValue));
            options.close();
          }
        }
      } else if (!srcIndex.isExpired(value)) {
        // unexpired PUT entry.
        if (deletesInEffect) {
          FileSpan deleteSearchSpan = new FileSpan(indexSegmentStartOffset, cutoffOffsetForDelete);
          IndexValue searchedValue = srcIndex.findKey(indexEntry.getKey(), deleteSearchSpan);
          if (!searchedValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
            // PUT entry that has not expired and is not considered deleted.
            validEntries.add(indexEntry);
          } else {
            logger.trace("Skipping {} because it is a deleted PUT", indexEntry.getKey());
          }
        } else {
          // valid PUT entry
          validEntries.add(indexEntry);
        }
      } else {
        logger.trace("Skipping {} because it is an expired PUT", indexEntry.getKey());
      }
    }
    return validEntries;
  }

  /**
   * Checks if a record already exists in the target log (swap spaces).
   * @param key the {@link StoreKey} of the record.
   * @param srcValue the {@link IndexValue} obtained from the application index.
   * @return {@code true} if the record already exists in the target log, {@code false} otherwise.
   * @throws StoreException if there is any problem with using the index.
   */
  private boolean alreadyExistsInTgt(StoreKey key, IndexValue srcValue) throws StoreException {
    IndexValue tgtValue = tgtIndex.findKey(key);
    return tgtValue != null && srcValue.isFlagSet(IndexValue.Flags.Delete_Index) == tgtValue.isFlagSet(
        IndexValue.Flags.Delete_Index);
  }

  /**
   * Copies the given {@code srcIndexEntries} from the given log segment into the swap spaces.
   * @param from the {@link LogSegment} to copy from.
   * @param srcIndexEntries the {@link IndexEntry}s to copy.
   * @param lastModifiedTimeSecs the last modified time of the source index segment.
   * @return @code true} if all the records  were copied. {@code false} if some records were not copied either because
   * there was no more capacity or because a shutdown was initiated.
   * @throws IOException if there were I/O errors during copying.
   * @throws StoreException if there are any problems reading or writing to store components.
   */
  private boolean copyRecords(LogSegment from, List<IndexEntry> srcIndexEntries, long lastModifiedTimeSecs)
      throws IOException, StoreException {
    boolean copiedAll = true;
    long totalCapacity = tgtLog.getCapacityInBytes();
    FileChannel fileChannel = Utils.openChannel(from.getView().getFirst(), false);
    long writtenLastTime = 0;
    for (IndexEntry srcIndexEntry : srcIndexEntries) {
      IndexValue srcValue = srcIndexEntry.getValue();
      long usedCapacity = tgtIndex.getLogUsedCapacity();
      if (isActive && tgtLog.getCapacityInBytes() - usedCapacity >= srcValue.getSize()) {
        fileChannel.position(srcValue.getOffset().getOffset());
        Offset endOffsetOfLastMessage = tgtLog.getEndOffset();
        // call into diskIOScheduler to make sure we can proceed (assuming it won't be 0).
        diskIOScheduler.getSlice(LOG_SEGMENT_COPY_JOB_NAME, LOG_SEGMENT_COPY_JOB_NAME, writtenLastTime);
        tgtLog.appendFrom(fileChannel, srcValue.getSize());
        FileSpan fileSpan = tgtLog.getFileSpanForMessage(endOffsetOfLastMessage, srcValue.getSize());
        IndexValue tgtValue;
        if (srcValue.isFlagSet(IndexValue.Flags.Delete_Index)) {
          IndexValue putValue = tgtIndex.findKey(srcIndexEntry.getKey());
          if (putValue != null && putValue.getOffset().getName().equals(fileSpan.getStartOffset().getName())) {
            tgtValue = new IndexValue(putValue.getSize(), putValue.getOffset(), putValue.getExpiresAtMs());
            tgtValue.setNewOffset(fileSpan.getStartOffset());
            tgtValue.setNewSize(srcValue.getSize());
          } else {
            tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getExpiresAtMs());
            tgtValue.clearOriginalMessageOffset();
          }
          tgtValue.setFlag(IndexValue.Flags.Delete_Index);
        } else {
          tgtValue = new IndexValue(srcValue.getSize(), fileSpan.getStartOffset(), srcValue.getExpiresAtMs());
        }
        IndexEntry entry = new IndexEntry(srcIndexEntry.getKey(), tgtValue);
        tgtIndex.addToIndex(entry, fileSpan);
        if (tgtValue.getOperationTimeInMs() == Utils.Infinite_Time) {
          tgtIndex.getIndexSegments().lastEntry().getValue().setLastModifiedTimeSecs(lastModifiedTimeSecs);
        }
        writtenLastTime = srcValue.getSize();
        logger.trace("Store : {} message set written to index ", dataDir);
      } else if (!isActive) {
        logger.info("Stopping copying because shutdown is in progress");
        copiedAll = false;
        break;
      } else {
        // this is the extra segment, so it is ok to run out of space.
        logger.info("There is no more capacity in the destination log. Total capacity is {}. Used capacity is {}. "
            + "Segment that was being copied is {}", totalCapacity, usedCapacity, from.getName());
        copiedAll = false;
        break;
      }
    }
    from.closeView();
    return copiedAll;
  }

  // commit() helpers

  /**
   * Gets the names of the target log segments that were generated (the swap spaces). Note that this function returns
   * those log segments that already exist and is not useful to determine what should exist and it only returns the ones
   * that are still in their temporary uncommitted state.
   * @return the list of target log segments that exist on the disk.
   */
  private List<String> getTargetLogSegmentNames() {
    List<String> filenames = Arrays.asList(dataDir.list(TEMP_LOG_SEGMENTS_FILTER));
    List<String> names = new ArrayList<>(filenames.size());
    for (String filename : filenames) {
      String transformed = filename.substring(0, filename.length() - TEMP_LOG_SEGMENT_NAME_SUFFIX.length());
      names.add(LogSegmentNameHelper.nameFromFilename(transformed));
    }
    Collections.sort(names, LogSegmentNameHelper.COMPARATOR);
    return names;
  }

  /**
   * Renames the temporary log segment files to remove the temporary file name suffix and effectively commits them on
   * disk.
   * @param logSegmentNames the names of the log segments whose backing files need to be renamed.
   * @throws StoreException if the rename fails.
   */
  private void renameLogSegments(List<String> logSegmentNames) throws StoreException {
    for (String segmentName : logSegmentNames) {
      String filename = LogSegmentNameHelper.nameToFilename(segmentName);
      File from = new File(dataDir, filename + TEMP_LOG_SEGMENT_NAME_SUFFIX);
      File to = new File(dataDir, filename);
      if (!from.renameTo(to)) {
        // TODO: change error code
        throw new StoreException("Failed to rename " + from + " to " + to, StoreErrorCodes.Unknown_Error);
      }
    }
  }

  /**
   * Commits the log segments with names {@code logSegmentNames}. Committing involves adding them to the application log
   * instance.
   * @param logSegmentNames the names of the log segments to commit.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException if there were any I/O errors creating a log segment
   */
  private void commitLogSegments(List<String> logSegmentNames, boolean recovering) throws IOException {
    for (String logSegmentName : logSegmentNames) {
      File segmentFile = new File(dataDir, LogSegmentNameHelper.nameToFilename(logSegmentName));
      LogSegment segment = new LogSegment(logSegmentName, segmentFile, metrics);
      srcLog.addSegment(segment, recovering);
    }
  }

  /**
   * Commits all the index segments that refer to log segments in {@code logSegmentNames}. The commit is atomic with
   * the use of {@link PersistentIndex#changeIndexSegments(List, Set)}.
   * @param logSegmentNames the names of the log segments whose index segments need to be committed.
   * @throws IOException if there were any I/O errors in setting up the index segment.
   * @throws StoreException if there were any problems committing the changed index segments.
   */
  private void commitIndexSegments(List<String> logSegmentNames) throws IOException, StoreException {
    Set<Offset> indexSegmentsToRemove = new HashSet<>();
    for (String logSegmentName : compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction()) {
      indexSegmentsToRemove.addAll(getIndexSegmentDetails(logSegmentName).keySet());
    }

    List<File> indexSegmentFilesToAdd = new ArrayList<>();
    Map<String, NavigableMap<Offset, File>> newLogSegmentsToIndexSegmentFiles =
        getLogSegmentsToIndexSegmentsMap(logSegmentNames);
    for (Map.Entry<String, NavigableMap<Offset, File>> entry : newLogSegmentsToIndexSegmentFiles.entrySet()) {
      LogSegment logSegment = srcLog.getSegment(entry.getKey());
      NavigableMap<Offset, File> indexSegmentDetails = entry.getValue();
      long endOffset = logSegment.getStartOffset();
      for (Map.Entry<Offset, File> indexSegmentEntry : indexSegmentDetails.entrySet()) {
        File indexSegmentFile = indexSegmentEntry.getValue();
        IndexSegment indexSegment =
            new IndexSegment(indexSegmentFile, true, storeKeyFactory, config, metrics, null, time);
        endOffset = indexSegment.getEndOffset().getOffset();
        indexSegmentFilesToAdd.add(indexSegmentFile);
      }
      logSegment.setEndOffset(endOffset);
    }
    srcIndex.changeIndexSegments(indexSegmentFilesToAdd, indexSegmentsToRemove);
  }

  /**
   * Gets a map of all the index segment files that cover the given {@code logSegmentNames}.
   * @param logSegmentNames the names of log segments whose index segment files are required.
   * @return a map keyed on log segment name and containing details about all the index segment files that refer to it.
   */
  private Map<String, NavigableMap<Offset, File>> getLogSegmentsToIndexSegmentsMap(List<String> logSegmentNames) {
    Map<String, NavigableMap<Offset, File>> logSegmentsToIndexSegmentsMap = new HashMap<>();
    for (String logSegmentName : logSegmentNames) {
      logSegmentsToIndexSegmentsMap.put(logSegmentName, getIndexSegmentDetails(logSegmentName));
    }
    return logSegmentsToIndexSegmentsMap;
  }

  // cleanup() helpers

  /**
   * Waits until the ref counts of the all the log segments that need to be cleaned up reach 0.
   * @throws InterruptedException if the wait is interrupted.
   */
  private void waitTillRefCountZero() throws InterruptedException {
    if (isActive) {
      long singleWaitMs = Math.min(500, WAIT_TIME_FOR_CLEANUP_MS);
      long waitSoFar = 0;
      List<String> segmentsUnderCompaction = compactionLog.getCompactionDetails().getLogSegmentsUnderCompaction();
      for (String segmentName : segmentsUnderCompaction) {
        while (srcLog.getSegment(segmentName).refCount() > 0 && waitSoFar < WAIT_TIME_FOR_CLEANUP_MS) {
          time.sleep(singleWaitMs);
          waitSoFar += singleWaitMs;
        }
      }
    }
  }

  /**
   * Cleans up all the log and index segments. This involves:
   * 1. Dropping the log segments that are no longer relevant.
   * 2. Deleting all associated index segment and bloom files.
   * @param recovering {@code true} if this function was called in the context of recovery. {@code false} otherwise.
   * @throws IOException
   */
  private void cleanupLogAndIndexSegments(boolean recovering) throws IOException {
    CompactionDetails details = compactionLog.getCompactionDetails();
    List<String> segmentsUnderCompaction = details.getLogSegmentsUnderCompaction();
    for (int i = 0; i < segmentsUnderCompaction.size(); i++) {
      String segmentName = segmentsUnderCompaction.get(i);
      if (srcLog.getSegment(segmentName) != null) {
        PersistentIndex.cleanupIndexSegmentFilesForLogSegment(dataDir.getAbsolutePath(), segmentName);
        srcLog.dropSegment(segmentName, recovering || i >= numSwapsUsed);
      }
    }
  }

  /**
   * Resets all the internal data structures and wipes the slate clean for the next compaction.
   */
  private void resetStructures() {
    tgtLog = null;
    tgtIndex = null;
    numSwapsUsed = 0;
    recoveryStartToken = null;
  }

  /**
   * Ends compaction by closing the compaction log if it exists.
   */
  private void endCompaction() {
    if (compactionLog != null) {
      compactionLog.close();
      compactionLog = null;
    }
  }

  // general helpers

  /**
   * Gets all the index segment files that cover the given {@code logSegmentName}.
   * @param logSegmentName the name of the log segment whose index segment files are required.
   * @return a map that contains all the index segment files of the log segment - keyed on the start offset.
   */
  private NavigableMap<Offset, File> getIndexSegmentDetails(String logSegmentName) {
    NavigableMap<Offset, File> indexSegmentFileToStartOffset = new TreeMap<>();
    File[] indexSegmentFiles =
        PersistentIndex.getIndexSegmentFilesForLogSegment(dataDir.getAbsolutePath(), logSegmentName);
    for (File indexSegmentFile : indexSegmentFiles) {
      indexSegmentFileToStartOffset.put(IndexSegment.getIndexSegmentStartOffset(indexSegmentFile.getName()),
          indexSegmentFile);
    }
    return indexSegmentFileToStartOffset;
  }
}