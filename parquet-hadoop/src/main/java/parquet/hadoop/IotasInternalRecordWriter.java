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
package parquet.hadoop;

import parquet.column.ParquetProperties;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Extension of InternalParquetRecordWriter.
 *
 * This extension prevents flush from within the write. Provides a way to determine
 * the current size of column chunks. It also overloads close to accept embedded table metadata
 * to be added to the footer.
 *
 * Created by Jaspreet Singh.
 */
class IotasInternalRecordWriter<T> extends InternalParquetRecordWriter<T> {

  /**
   * @param parquetFileWriter  the file to write to
   * @param writeSupport       the class to convert incoming records
   * @param schema             the schema of the records
   * @param extraMetaData      extra meta data to write in the footer of the file
   * @param rowGroupSize       the size of a block in the file (this will be approximate)
   * @param pageSize           the size of page in the file
   * @param compressor         the codec used to compress
   * @param dictionaryPageSize the size of dicationary pages.
   */
  public IotasInternalRecordWriter(ParquetFileWriter parquetFileWriter,
                                   WriteSupport<T> writeSupport,
                                   MessageType schema, Map<String,
      String> extraMetaData,
                                   long rowGroupSize,
                                   int pageSize,
                                   CodecFactory.BytesCompressor compressor,
                                   int dictionaryPageSize,
                                   boolean enableDictionary,
                                   boolean validating,
                                   ParquetProperties.WriterVersion writerVersion) {
    super(parquetFileWriter, writeSupport, schema, extraMetaData, rowGroupSize, pageSize,
        compressor, dictionaryPageSize, enableDictionary, validating, writerVersion);
  }

  /**
   * Overrides the write method to prevent flush from write. Client may use getCurrentSize to
   * determine whether to flush or not.
   */
  @Override
  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++recordCount;
  }

  /**
   * This method provides the current approximate size in memory.
   */
  public long getCurrentSize() {
    return columnStore.getAllocatedSize();
  }

  /**
   * While closing the main record writer, accepts metadata of the embedded tables
   * which needs to be written to the main footer.
   *
   * @param embeddedTableMetadata metadata from the embedded tables that needs to be written
   *                              to the main footer.
   * @throws IOException
   * @throws InterruptedException
   */

  public void close(Map<String, String> embeddedTableMetadata)
      throws IOException, InterruptedException {

    extraMetaData.putAll(embeddedTableMetadata);

    super.close();
  }

  /**
   * provides a way to flush without invoking startBlock and endBlock.
   */
  public void flushColumnStore() {
    columnStore.flush();
    columnStore = null;
  }

  /**
   * provides a way to flush page store without invoking startBlock and endBlock when
   * the caller may be writing embedded table.
   *
   * @throws IOException
   */
  public void flushPageStore() throws IOException {
    pageStore.flushToFileWriter(parquetFileWriter);
    pageStore = null;
  }

  public long getRecordCount() {
    return recordCount;
  }

}
