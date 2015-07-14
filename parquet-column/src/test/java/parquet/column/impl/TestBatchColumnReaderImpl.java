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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static parquet.io.api.GroupConverter.ROW_BATCH_SIZE;

import java.util.List;

import org.junit.Test;

import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.ParquetProperties;
import parquet.column.page.DataPage;
import parquet.column.page.DataPageV2;
import parquet.column.page.mem.MemPageReader;
import parquet.column.page.mem.MemPageWriter;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class TestBatchColumnReaderImpl {
  MessageType schema;
  ColumnDescriptor col;
  MemPageWriter pageWriter;
  ColumnWriterV2 columnWriterV2;
  MemPageReader pageReader;

  private int rows = 13001;

  private void reset() {
    schema = null;
    col = null;
    pageWriter = null;
    columnWriterV2 = null;
    pageReader = null;
  }

  private void setupPageWrite(String schemaDesc) {
    schema = MessageTypeParser.parseMessageType(schemaDesc);
    col = schema.getColumns().get(0);
    pageWriter = new MemPageWriter();
    columnWriterV2 = new ColumnWriterV2(
        col, pageWriter, new ParquetProperties(1024, PARQUET_2_0, true), 2048);
  }

  private void setupPageRead() {
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2)dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);
    pageReader = new MemPageReader((long)rows, pages.iterator(), pageWriter.getDictionaryPage());
  }

  private static final class BatchConverter extends PrimitiveConverter {
    int count;
    boolean[] nullStore;
    Binary[] binaryStore;
    String[] convertedValueStore;
    int filled; // number of valid elements in value store
    int[] dictLookupIdStore;
    Dictionary dict;
    String[] convertedDict;

    @Override
    public boolean hasDictionarySupport() { return true; }

    @Override
    public boolean hasBatchSupport() { return true; }

    @Override
    public int[] getDictLookupBatchStore(int maxBatchSize) {
      if (dictLookupIdStore == null || maxBatchSize > dictLookupIdStore.length) {
        dictLookupIdStore = new int[maxBatchSize];
        convertedValueStore = new String[maxBatchSize];
      }
      return dictLookupIdStore;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = dictionary;
      convertedDict = new String[dict.getMaxId() + 1];
      for (int i = 0; i < convertedDict.length; ++i)
        convertedDict[i] = dict.decodeToBinary(i).toStringUsingUTF8();
    }

    @Override
    public boolean[] getNullIndicatorBatchStore(int maxBatchSize) {
      if (nullStore == null || maxBatchSize > nullStore.length)
        nullStore = new boolean[maxBatchSize];
      return nullStore;
    }

    @Override
    public Binary[] getBinaryBatchStore(int maxBatchSize) {
      if (binaryStore == null || maxBatchSize > binaryStore.length) {
        binaryStore = new Binary[maxBatchSize];
        convertedValueStore = new String[maxBatchSize];
      }
      return binaryStore;
    }

    @Override
    public void startOfBatchOp() {
      for (int i = 0; i < filled; ++i)
        convertedValueStore[i] = null;
      filled = 0;
    }

    @Override
    public void endOfBatchOp(int filledBatchSize) {
      filled = filledBatchSize;
      count += filledBatchSize;
    }

    @Override
    public void endOfDictBatchOp(int filledBatchSize) {
      for (int i = 0; i < filledBatchSize; ++i){
        convertedValueStore[i] = convertedDict[dictLookupIdStore[i]];
      }
      filled = filledBatchSize;
      count += filledBatchSize;
    }

    String getBatchValueAt(int i) {
      if (nullStore[i]) return null;
      String s = convertedValueStore[i];
      if (s == null) {
        s = binaryStore[i].toStringUsingUTF8();
        convertedValueStore[i] = s;
      }
      return s;
    }
  }

  @Test
  public void test() {
    reset();
    setupPageWrite("message test { required binary foo; }");
    for (int i = 0; i < rows; i++) {
      columnWriterV2.write(Binary.fromString("bar" + i % 10), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    columnWriterV2.writePage(rows);
    columnWriterV2.finalizeColumnChunk();

    BatchColumnReader colReader;

    // test with a converter
    setupPageRead();
    BatchConverter converter = new BatchConverter();
    colReader = new BatchColumnReaderImpl(col, pageReader, converter);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        assertFalse(colReader.isValueNull());
        String expectedVal = "bar" + i % 10;
        assertEquals(expectedVal, colReader.getBinary().toStringUsingUTF8());
        assertEquals(expectedVal, converter.getBatchValueAt(j));
        colReader.consume();
      }
    }
    assertEquals(rows, converter.count);

    // test without a converter
    setupPageRead();
    colReader = new BatchColumnReaderImpl(col, pageReader);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        assertFalse(colReader.isValueNull());
        String expectedVal = "bar" + i % 10;
        assertEquals(expectedVal, colReader.getBinary().toStringUsingUTF8());
        colReader.consume();
      }
    }

  }

  @Test
  public void testOptional() {
    reset();
    setupPageWrite("message test { optional binary foo; }");

    final int DICT_ENTRY_COUNT = 32;
    for (int i = 0; i < rows; i++) {
      if (i % 2 == 0)
        columnWriterV2.write(Binary.fromString("bar" + i % DICT_ENTRY_COUNT), 0, 1);
      else
        columnWriterV2.writeNull(0, 0);
      if ((i + 1) % 200 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    columnWriterV2.writePage(rows);
    columnWriterV2.finalizeColumnChunk();

    BatchColumnReader colReader;

    // test with a converter
    setupPageRead();
    BatchConverter converter = new BatchConverter();
    colReader = new BatchColumnReaderImpl(col, pageReader, converter);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        if (i % 2 == 0) {
          String expectedVal = "bar" + i % DICT_ENTRY_COUNT;
          assertEquals(expectedVal, colReader.getBinary().toStringUsingUTF8());
          assertEquals(expectedVal, converter.getBatchValueAt(j));
        } else {
          assertTrue(colReader.isValueNull());
          assertNull(converter.getBatchValueAt(j));
        }
        colReader.consume();
      }
    }
    assertEquals(rows, converter.count);

    // test without a converter
    setupPageRead();
    colReader = new BatchColumnReaderImpl(col, pageReader);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        if (i % 2 == 0) {
          String expectedVal = "bar" + i % DICT_ENTRY_COUNT;
          assertEquals(expectedVal, colReader.getBinary().toStringUsingUTF8());
        } else {
          assertTrue(colReader.isValueNull());
        }
        colReader.consume();
      }
    }

  }

  @Test
  public void testDictToNonDictSwitch() {
    reset();
    setupPageWrite("message test { required binary foo; }");

    final int DICT_ENTRY_COUNT1 = 32;
    final int DICT_ENTRY_COUNT2 = 256;
    final int pageRowCount = 1000;
    for (int i = 0; i < pageRowCount; ++i) {
      columnWriterV2.write(Binary.fromString("bar" + i % DICT_ENTRY_COUNT1), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    for (int i = pageRowCount; i < rows; i++) {
      columnWriterV2.write(Binary.fromString("bar" + i % DICT_ENTRY_COUNT2), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage(i);
      }
    }
    columnWriterV2.writePage(rows);
    columnWriterV2.finalizeColumnChunk();

    BatchColumnReader colReader;

    // test with a converter
    setupPageRead();
    BatchConverter converter = new BatchConverter();
    colReader = new BatchColumnReaderImpl(col, pageReader, converter);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        assertFalse(colReader.isValueNull());
        String expectedVal;
        if (i < pageRowCount)
          expectedVal = "bar" + i % DICT_ENTRY_COUNT1;
        else
          expectedVal = "bar" + i % DICT_ENTRY_COUNT2;
        String colValue = colReader.getBinary().toStringUsingUTF8();
        assertEquals(expectedVal, colValue);
        assertEquals(expectedVal, converter.getBatchValueAt(j));
        colReader.consume();
      }
    }
    assertEquals(rows, converter.count);

    // test without a converter
    setupPageRead();
    colReader = new BatchColumnReaderImpl(col, pageReader);
    for (int i = 0; i < rows;) {
      int batchSize = Math.min(colReader.getRemainingPageValueCount(), ROW_BATCH_SIZE);
      int filledSize = colReader.writeCurrentBatchToConverter(batchSize);
      for (int j = 0; j < filledSize; ++j, ++i) {
        assertFalse(colReader.isValueNull());
        String expectedVal;
        if (i < pageRowCount)
          expectedVal = "bar" + i % DICT_ENTRY_COUNT1;
        else
          expectedVal = "bar" + i % DICT_ENTRY_COUNT2;
        String colValue = colReader.getBinary().toStringUsingUTF8();
        assertEquals(expectedVal, colValue);
        colReader.consume();
      }
    }
  }
}
