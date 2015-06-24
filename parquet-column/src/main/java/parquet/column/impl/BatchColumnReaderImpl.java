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
package parquet.column.impl;

import static java.lang.String.format;
import static parquet.Log.DEBUG;
import static parquet.Preconditions.checkArgument;
import static parquet.Preconditions.checkNotNull;
import static parquet.column.ValuesType.DEFINITION_LEVEL;
import static parquet.column.ValuesType.REPETITION_LEVEL;
import static parquet.column.ValuesType.VALUES;
import static parquet.column.impl.ColumnReaderImpl.IntIterator;
import static parquet.column.impl.ColumnReaderImpl.RLEIntIterator;
import static parquet.column.impl.ColumnReaderImpl.ValuesReaderIntIterator;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageReader;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;

/**
 * BatchColumnReader implementation based on ColumnReaderImpl.
 *
 *
 * @author Julien Le Dem
 * @author Prakash Das
 *
 */
public class BatchColumnReaderImpl implements BatchColumnReader {
  private static final Log LOG = Log.getLog(BatchColumnReaderImpl.class);

  /**
   * binds the lower level page decoder to the record converter materializing the records
   */
  private static abstract class BatchBinding {

    /**
     * read one value from the underlying page
     */
    abstract void read();

    /**
     * read a batch of values from the underlying page
     * @param batchSize the number of values that shouldn't exceed the
     * number of values remaining in the page.
     */
    abstract void readNextBatch(int batchSize);

    /**
     * write current value to converter
     */
    abstract void writeValue();

    /**
     * @return true if currently reading dictionary lookup values
     */
    public boolean isUsingDictionary() {
      return false;
    }

    /**
     * @return current value
     */
    public int getDictionaryId() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public int getInteger() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public boolean getBoolean() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public long getLong() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public Binary getBinary() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public float getFloat() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public double getDouble() {
      throw new UnsupportedOperationException();
    }
  }

  private final ColumnDescriptor path;
  private final long totalValueCount;
  private final PageReader pageReader;
  private final Dictionary dictionary;

  private IntIterator definitionLevelColumn;
  protected ValuesReader dataColumn;

  private int definitionLevel;
  private final int maxBatchSize;
  /*
   * One or more skip operations followed by a read batch operation will
   * read only a single value. A subsequent read without any intervening
   * skip operation will read a batch of values. This heuristics is
   * implemented by the following flag.
   */
  private boolean honorBatchRead = true;
  private int readIndex = 0;
  private int filledBatchSize = 0;
  /*
   * Number of values skipped after the current batch of values has been read or
   * skipped. This keeps track where the read value will start.
   */
  private int skipCount = 0;
  private int[] dictionaryIds;
  private boolean[] nullFlags;

  private long endOfPageValueCount;
  private int readValues;
  private int pageValueCount;

  private final PrimitiveConverter converter;
  private BatchBinding binding;


  private void bindToDictionary(final Dictionary dictionary) {

    binding = new BatchBinding() {
      int curDictId;

      public boolean isUsingDictionary() {
        return true;
      }

      void read() {
        curDictId = dictionaryIds[readIndex];
      }

      public int getDictionaryId() {
        return curDictId;
      }

      void writeValue() {
        converter.addValueFromDictionary(curDictId);
      }

      public int getInteger() {
        return dictionary.decodeToInt(curDictId);
      }

      public boolean getBoolean() {
        return dictionary.decodeToBoolean(curDictId);
      }

      public long getLong() {
        return dictionary.decodeToLong(curDictId);
      }

      public Binary getBinary() {
        return dictionary.decodeToBinary(curDictId);
      }

      public float getFloat() {
        return dictionary.decodeToFloat(curDictId);
      }

      public double getDouble() {
        return dictionary.decodeToDouble(curDictId);
      }

      @Override
      void readNextBatch(int size) {
        for (int i = 0; i < size; i++) {
          readDefinitionLevelOnly();
          boolean b = definitionLevel < path.getMaxDefinitionLevel();
          nullFlags[i] = b;
          if (!b) {
            dictionaryIds[i] = dataColumn.readValueDictionaryId();
          }
        }
      }
    };
  }

  private void bind(PrimitiveTypeName type) {
    if (binding == null) {
      binding = type
          .convert(new PrimitiveTypeNameConverter<BatchBinding, RuntimeException>() {
            @Override
            public BatchBinding convertFLOAT(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                float[] batchStore = converter.getFloatBatchStore(maxBatchSize);
                float curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      batchStore[i] = dataColumn.readFloat();
                    }
                  }
                }

                public float getFloat() {
                  return curValue;
                }

                void writeValue() {
                  converter.addFloat(curValue);
                }
              };
            }

            @Override
            public BatchBinding convertDOUBLE(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                double[] batchStore = converter.getDoubleBatchStore(maxBatchSize);
                double curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                public double getDouble() {
                  return curValue;
                }

                void writeValue() {
                  converter.addDouble(curValue);
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      batchStore[i] = dataColumn.readDouble();
                    }
                  }
                }
             };
            }

            @Override
            public BatchBinding convertINT32(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                int[] batchStore = converter.getIntBatchStore(maxBatchSize);
                int curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                @Override
                public int getInteger() {
                  return curValue;
                }

                void writeValue() {
                  converter.addInt(curValue);
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      batchStore[i] = dataColumn.readInteger();
                    }
                  }
                }
              };
            }

            @Override
            public BatchBinding convertINT64(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                long[] batchStore = converter.getLongBatchStore(maxBatchSize);
                long curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                @Override
                public long getLong() {
                  return curValue;
                }

                void writeValue() {
                  converter.addLong(curValue);
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      batchStore[i] = dataColumn.readLong();
                    }
                  }
                }
              };
            }

            @Override
            public BatchBinding convertINT96(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return this.convertBINARY(primitiveTypeName);
            }

            @Override
            public BatchBinding convertFIXED_LEN_BYTE_ARRAY(
                PrimitiveTypeName primitiveTypeName) throws RuntimeException {
              return this.convertBINARY(primitiveTypeName);
            }

            @Override
            public BatchBinding convertBOOLEAN(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                boolean[] batchStore = converter.getBooleanBatchStore(maxBatchSize);
                boolean curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                @Override
                public boolean getBoolean() {
                  return curValue;
                }

                void writeValue() {
                  converter.addBoolean(curValue);
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      batchStore[i] = dataColumn.readBoolean();
                    }
                  }
                }
              };
            }

            @Override
            public BatchBinding convertBINARY(PrimitiveTypeName primitiveTypeName)
                throws RuntimeException {
              return new BatchBinding() {
                Binary[] batchStore = converter.getBinaryBatchStore(maxBatchSize);
                Binary curValue;

                void read() {
                  curValue = batchStore[readIndex];
                }

                @Override
                public Binary getBinary() {
                  return curValue;
                }

                void writeValue() {
                  converter.addBinary(curValue);
                }

                @Override
                void readNextBatch(int size) {
                  for (int i = 0; i < size; i++) {
                    readDefinitionLevelOnly();
                    boolean b = definitionLevel < path.getMaxDefinitionLevel();
                    nullFlags[i] = b;
                    if (!b) {
                      Binary readData = dataColumn.readBytes();
                      batchStore[i] = readData;
                    }
                  }
                }
              };
            }
          });
    }
  }

  /**
   * creates a reader for triplets
   * @param path the descriptor for the corresponding column
   * @param pageReader the underlying store to read from
   */
  public BatchColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter) {
    this.path = checkNotNull(path, "path");
    this.pageReader = checkNotNull(pageReader, "pageReader");
    this.converter = checkNotNull(converter, "converter");
    checkArgument(converter.hasBatchSupport(), "converter must support batch operation");
    if (path.getMaxRepetitionLevel() > 0)
      LOG.warn("repetition level values are not available through this class: " + getClass().getName());
    maxBatchSize = GroupConverter.ROW_BATCH_SIZE;
    this.nullFlags = converter.getNullIndicatorBatchStore(maxBatchSize);
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
        if (converter.hasDictionarySupport()) {
          this.dictionaryIds = converter.getDictLookupBatchStore(maxBatchSize);
          converter.setDictionary(dictionary);
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
      }
    } else {
      this.dictionary = null;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new ParquetDecodingException("totalValueCount == 0");
    }
    readPageIfRequired();
  }

  private boolean isFullyConsumed() {
    return readValues >= totalValueCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#writeCurrentValueToConverter()
   */
  @Override
  public void writeCurrentValueToConverter() {
    readValue();
    this.binding.writeValue();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.BatchColumnReader#writeCurrentBatchToConverter()
   */
  @Override
  public int writeCurrentBatchToConverter(int batchSize) {
    accountForSkippedValues();
    int pageValuesRemaining = (int) (endOfPageValueCount - readValues + 1);
    checkArgument(batchSize <= pageValuesRemaining,
        "trying to read %d values out of %d remaining values in current page",
        batchSize, pageValuesRemaining);
    converter.startOfBatchOp();
    // Binding to converter may change when a column switches from dictionary
    // encoding to non-dictionary encoding at a page boundary. This is also
    // the reason for the above check to keep implementation simple for the
    // converters & the subsequent access of converted values.
    BatchBinding prevBinding = binding;
    readIndex = 0;
    if (!honorBatchRead) batchSize = 1;
    readNextBatch(batchSize);
    filledBatchSize = batchSize;
    if (prevBinding.isUsingDictionary())
      converter.endOfDictBatchOp(batchSize);
    else
      converter.endOfBatchOp(batchSize);
    honorBatchRead = true;
    return batchSize;
  }

  @Override
  public int getCurrentValueDictionaryID() {
    readValue();
    return binding.getDictionaryId();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    readValue();
    return this.binding.getInteger();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    readValue();
    return this.binding.getBoolean();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getLong()
   */
  @Override
  public long getLong() {
    readValue();
    return this.binding.getLong();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    readValue();
    return this.binding.getBinary();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    readValue();
    return this.binding.getFloat();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    readValue();
    return this.binding.getDouble();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentRepetitionLevel()
   */
  @Override
  public int getCurrentRepetitionLevel() {
    // supports reading only a column with no repeated values.
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getDescriptor()
   */
  @Override
  public ColumnDescriptor getDescriptor() {
    return path;
  }

  /**
   * Reads the value into the binding.
   */
  public void readValue() {
    if (readIndex >= filledBatchSize) {
      throw new ParquetDecodingException(
          format("Batch is empty. Initiate a fill batch operation invoking writeCurrentBatchToConverter method"));
    }
    binding.read();
  }

  private void readNextBatch(int batchSize) {
    try {
      binding.readNextBatch(batchSize);
      readPageIfRequired();
    } catch (RuntimeException e) {
      format("Cauld not read a batch of values for column %s", path.getPath().toString(), e);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * Note: This skips values beyond what has already been read
   * into a batch irrespective of whether those values in the batch has been
   * consumed or not.
   * @see parquet.column.ColumnReader#skip()
   */
  @Override
  public void skip() {
    ++skipCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getCurrentDefinitionLevel()
   */
  @Override
  public int getCurrentDefinitionLevel() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  private void accountForSkippedValues() {
    if (skipCount == 0) return;
    honorBatchRead = false;
    do {
      // consume the current page fully (logically)
      int toBeConsumed = (int)(endOfPageValueCount - readValues); // safe cast
      if (toBeConsumed > skipCount) {
        // in order to skip (value, definition level, repetition level) in a page
        // partially, you need to read and discard them.
        while (skipCount > 0) {
          int batchSize = Math.min(maxBatchSize, skipCount);
          readNextBatch(batchSize);
          skipCount -= batchSize;
        }
      } else { // we can discard the rest of the page safely without actually reading.
        skipCount -= toBeConsumed;
        readValues += toBeConsumed;
        // read the next page to discard remaining skipCount, if any.
        readPageIfRequired();
      }
    } while (skipCount > 0);
    assert(skipCount == 0);
  }

  private void readDefinitionLevelOnly() {
    definitionLevel = definitionLevelColumn.nextInt();
    ++readValues;
  }

  private void readPageIfRequired() {
    if (isPageFullyConsumed()) {
      if (isFullyConsumed()) {
        if (DEBUG) LOG.debug("end reached");
        return;
      }
      readPage();
    }
  }

  private void readPage() {
    if (DEBUG) LOG.debug("loading page");
    DataPage page = pageReader.readPage();
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        readPageV1(dataPageV1);
        return null;
      }
      @Override
      public Void visit(DataPageV2 dataPageV2) {
        readPageV2(dataPageV2);
        return null;
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount) {
    this.pageValueCount = valueCount;
    this.endOfPageValueCount = readValues + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException(
            "could not read page in col " + path + " as the dictionary was missing for encoding " + dataEncoding);
      }
      this.dataColumn = dataEncoding.getDictionaryBasedValuesReader(path, VALUES, dictionary);
    } else {
      this.dataColumn = dataEncoding.getValuesReader(path, VALUES);
    }
    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
      bindToDictionary(dictionary);
    } else {
      // if it is a switch from dictionary to non-dictionary, re-initialize binding
      if (dictionary != null) binding = null;
      bind(path.getType());
    }
    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page in col " + path, e);
    }
  }

  private void readPageV1(DataPageV1 page) {
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(path, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(path, DEFINITION_LEVEL);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      if (DEBUG) LOG.debug("page size " + bytes.length + " bytes and " + pageValueCount + " records");
      if (DEBUG) LOG.debug("reading repetition levels at 0");
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      if (DEBUG) LOG.debug("reading definition levels at " + next);
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      if (DEBUG) LOG.debug("reading data at " + next);
      initDataReader(page.getValueEncoding(), bytes, next, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
  }

  private void readPageV2(DataPageV2 page) {
    this.definitionLevelColumn = newRLEIterator(path.getMaxDefinitionLevel(), page.getDefinitionLevels());
    try {
      if (DEBUG) LOG.debug("page data size " + page.getData().size() + " bytes and " + pageValueCount + " records");
        initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
  }

  private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new NullIntIterator();
      }
      return new RLEIntIterator(
          new RunLengthBitPackingHybridDecoder(
              BytesUtils.getWidthFromMaxInt(maxLevel),
              new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read levels in page for col " + path, e);
    }
  }

  private static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() {
      return 0;
    }
  }

  private boolean isPageFullyConsumed() {
    return readValues >= endOfPageValueCount;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#consume()
   */
  @Override
  public void consume() {
    if (readIndex >= filledBatchSize) {
      throw new ParquetDecodingException(
          format("Batch is empty. Initiate a fill batch operation invoking writeCurrentBatchToConverter method"));
    }
    ++readIndex;
  }


  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#getTotalValueCount()
   */
  @Override
  public long getTotalValueCount() {
    return totalValueCount;
  }

  /**
   * {@inheritDoc}
   * Note: The returned count excludes the values that are in the
   * current batch but yet to be read from the batch.
   * @see parquet.column.ColumnReader#getRemainingValueCount()
   */
  @Override
  public long getRemainingValueCount() {
    return (totalValueCount - readValues);
  }

  /**
   * {@inheritDoc}
   * Note: The returned count excludes the values that are in the
   * current batch but yet to be read from the batch.
   * @see parquet.column.ColumnReader#getRemainingPageValueCount()
   */
  @Override
  public int getRemainingPageValueCount() {
    accountForSkippedValues();
    return (int)(endOfPageValueCount - readValues);
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.ColumnReader#isValueNull()
   */
  @Override
  public boolean isValueNull() {
    if (readIndex >= filledBatchSize) {
      throw new ParquetDecodingException(
          format("Batch is empty. Initiate a fill batch operation invoking writeCurrentBatchToConverter method"));
    }
    return nullFlags[readIndex]; 
  }

}
