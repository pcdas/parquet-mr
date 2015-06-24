/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.io;

import parquet.Log;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;


/**
 * used to read reassembled records
 * @author Julien Le Dem
 * @author Prakash Das
 * 
 * @param <T> the type of the materialized record
 */
class RecordReaderBatchImplementation<T> extends RecordReaderImplementation<T> {
  private static final Log LOG = Log.getLog(RecordReaderBatchImplementation.class);

  private final GroupConverter rootConverter;
  private final boolean isFlatRecord;
  private final boolean useBatchedRead;

  private int entriesInLastBatch = 0;
  private int indexOfLastEntryReturned = 0;

  /**
   * @param root the root of the schema
   * @param recordMaterializer responsible of materializing the records
   * @param validating whether we should validate against the schema
   * @param columnStore where to read the column data from
   */
  public RecordReaderBatchImplementation(MessageColumnIO root,
                                         RecordMaterializer<T> recordMaterializer,
                                         boolean validating,
                                         ColumnReadStoreImpl columnStore,
                                         boolean useBatchedRead) {
    super(root, recordMaterializer, validating, columnStore);
    rootConverter = (GroupConverter)super.getRecordConsumer();
    // For a flat record, the read path for each value can be optimized. So,
    // pre-compute the result if the schema is flat.
    boolean result = true;
    State currentState = getState(0);
    do {
      if (currentState.maxRepetitionLevel != 0 ||
          currentState.maxDefinitionLevel > 1 ||
          currentState.fieldPath.length > 1) {
        result = false;
        break;
      }
      currentState = currentState.getNextState(0);
    } while (currentState != null);
    isFlatRecord = result;
    result = true;
    if (isFlatRecord) {
      currentState = getState(0);
      do {
        if (!currentState.primitiveConverter.hasBatchSupport()) {
          result = false;
          break;
        }
        currentState = currentState.getNextState(0);
      } while (currentState != null);
    }
    boolean hasBatchSupport = result;
    this.useBatchedRead = useBatchedRead && hasBatchSupport;
  }

  private void skipFlatRead() {
    rootConverter.start();
    State currentState = getState(0);
    do {
      BatchColumnReader columnReader = (BatchColumnReader)currentState.column;
      columnReader.skip();
      // in a flat schema no repetition is allowed
      currentState = currentState.getNextState(0);
    } while (currentState != null);
    rootConverter.end();
  }

  private T flatReadFromBatch() {
    T record = null;

    loadBatchIfNecessary();

    // return a row from the current batch
    record = getMaterializer().getCurrentRecord();
    indexOfLastEntryReturned++;

    shouldSkipCurrentRecord = record == null;
    if (shouldSkipCurrentRecord) {
        getMaterializer().skipCurrentRecord();
    }
    return record;
  }

  private void loadBatchIfNecessary() {
    // are all entries from previous batch already returned ? else load a new
    // batch
    if (indexOfLastEntryReturned == entriesInLastBatch) {
      log("entriesInLastBatch:" + entriesInLastBatch
          + " indexOfLastEntryReturned:" + indexOfLastEntryReturned);
      entriesInLastBatch = loadBatch();
      indexOfLastEntryReturned = 0;
    }
  }

  private void skipFlatReadFromBatch() {
    if (indexOfLastEntryReturned < entriesInLastBatch) {
      indexOfLastEntryReturned++;
      getMaterializer().skipCurrentRecord();
    } else {
      // Don't read ahead a batch while skipping
      skipFlatRead();
    }
  }

  /*
   * For a column, the converter associated with a column can change from dictionary based
   * to a non-dictionary based converter at a page boundary. At the implementation
   * level, it is easier to maintain a batch of rows if the converters remain same across
   * all row values.
   */
  private int calcBatchSize() {
    int bsize = GroupConverter.ROW_BATCH_SIZE;
    State currentState = getState(0);
    do {
      ColumnReader columnReader = currentState.column;
      bsize = Math.min(bsize, columnReader.getRemainingPageValueCount());
      currentState = currentState.getNextState(0);
    } while (currentState != null);
    return bsize;
  }

  private int loadBatch() {
    int batchSize = calcBatchSize();
    rootConverter.startBatch();
    State currentState = getState(0);
    do {
      BatchColumnReader columnReader = (BatchColumnReader)currentState.column;
      batchSize = columnReader.writeCurrentBatchToConverter(batchSize);
      // in a flat schema no repetition is allowed
      currentState = currentState.getNextState(0);
    } while (currentState != null);
    rootConverter.endBatch();
    // return the number of entries read in the batch
    return batchSize;
  }

  /**
   * @see parquet.io.RecordReader#read()
   */
  @Override
  public T read() {
    if (isFlatRecord && useBatchedRead)
      return flatReadFromBatch();

    return super.read();
  }

  /**
   * @see parquet.io.RecordReader#skip()
   */
  @Override
  public void skip() {
    if (isFlatRecord && useBatchedRead)
      skipFlatReadFromBatch();
    else
      skipNonBatchRecord();
  }
  
  /*
   * The following code logically belongs to super class implementation, but appears
   * in this class so that this customized code will not have any merge conflict
   * when upgrading to a later release. 
   */
  private void skipNonBatchRecord() {
    int currentLevel = 0;
    rootConverter.start();
    State currentState = getState(0);
    do {
      ColumnReader columnReader = currentState.column;
      int d = columnReader.getCurrentDefinitionLevel();
      // creating needed nested groups until the current field (opening tags)
      int depth = currentState.getDepth(d);
      for (; currentLevel <= depth; ++currentLevel) {
        currentState.groupConverterPath[currentLevel].start();
      }
      // currentLevel = depth + 1 at this point
      // set the current value
      if (d >= currentState.maxDefinitionLevel) {
        // not null
        columnReader.skip();
      }
      columnReader.consume();

      int nextR = currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
      // level to go to close current groups
      int next = currentState.nextLevel[nextR];
      for (; currentLevel > next; currentLevel--) {
        currentState.groupConverterPath[currentLevel - 1].end();
      }

      currentState = currentState.getNextState(nextR);
    } while (currentState != null);
    rootConverter.end();
    getMaterializer().skipCurrentRecord();
  }

  private static void log(String string) {
    LOG.debug(string);
  }

  protected GroupConverter getRecordConsumer() {
    return rootConverter;
  }

}
