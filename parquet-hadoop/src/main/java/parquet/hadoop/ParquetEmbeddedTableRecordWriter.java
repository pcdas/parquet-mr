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

import parquet.Log;
import parquet.column.ParquetProperties;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import parquet.hadoop.iotas.ParquetEmbeddedTableFileWriter;
import parquet.hadoop.metadata.EmbeddedTableMetadata;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Writes Embedded Table records and adds the relevant metadata for the embedded table to parquet
 * file metadata
 *
 * Created by Jaspreet Singh
 */
public class ParquetEmbeddedTableRecordWriter<E> {
  private static final Log LOG = Log.getLog(ParquetEmbeddedTableRecordWriter.class);

  private EmbeddedTableMetadata embeddedTableMetadata;

  private ParquetEmbeddedTableFileWriter embTableFileWriter;
  private IotasInternalRecordWriter<E> internalWriter;
  private WriteSupport<E> writeSupport;
  private MessageType schema;

  private Map<String, String> extraMetadata = new HashMap<String, String>();

  private long rowGroupSizeThreshold;


  /**
   * @param parquetFileWriter      the file to write to
   * @param writeSupport           the class to convert incoming records
   * @param schema                 the schema of the records
   * @param extraMetaData          extra meta data to write in the footer of the file
   * @param rowGroupSize           the size of a block in the file (this will be approximate)
   * @param compressor             the codec used to compress
   * @param embeddedTableMetadata  the metadata for the embedded table that needs to be written to
   *                               parquet file metadata
   */
  public ParquetEmbeddedTableRecordWriter(ParquetEmbeddedTableFileWriter parquetFileWriter,
                                          WriteSupport<E> writeSupport,
                                          MessageType schema,
                                          Map<String, String> extraMetaData,
                                          long rowGroupSize,
                                          int pageSize,
                                          CodecFactory.BytesCompressor compressor,
                                          int dictionaryPageSize,
                                          boolean enableDictionary,
                                          boolean validating,
                                          ParquetProperties.WriterVersion writerVersion,
                                          EmbeddedTableMetadata embeddedTableMetadata) {
    this.internalWriter = new IotasInternalRecordWriter<E>(parquetFileWriter, writeSupport,
        schema, extraMetaData, rowGroupSize, pageSize, compressor, dictionaryPageSize,
        enableDictionary, validating, writerVersion);

    this.embTableFileWriter = parquetFileWriter;
    this.rowGroupSizeThreshold = rowGroupSize;
    this.schema = schema;
    this.extraMetadata.putAll(extraMetaData);

    this.embeddedTableMetadata = embeddedTableMetadata;

    this.writeSupport = writeSupport;
  }

  /**
   * Flushes the embedded table to parquet file.
   *
   * @throws IOException
   */
  private void flushEmbeddedRowGroupToStore() throws IOException {
    long currentSize = internalWriter.getCurrentSize();
    long recordCount = internalWriter.getRecordCount();

    LOG.info(format("Flushing Embedded Table from mem columnStore to file. allocated memory: %,d",
        currentSize));
    if (currentSize > 3 * rowGroupSizeThreshold) {
      LOG.warn("Too much memory used: " + currentSize);
    }

    if (recordCount > 0) {
      embTableFileWriter.startEmbeddedTableBlock(recordCount);

      internalWriter.flushColumnStore();
      internalWriter.flushPageStore();

      embTableFileWriter.endEmbeddedTableBlock();
    }

  }

  /**
   * flushes the embedded table content to parquet file, writes the footer for embedded table
   * content and updates embeddedTableMetadata.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException {
    flushEmbeddedRowGroupToStore();
    FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();

    extraMetadata.putAll(finalWriteContext.getExtraMetaData());

    this.embeddedTableMetadata.setEmbeddedTableFooterPosition(embTableFileWriter.getPos());

    embTableFileWriter.writeEmbeddedTableFooter(schema, extraMetadata);
  }

  public EmbeddedTableMetadata getEmbeddedTableMetadata() {
    return embeddedTableMetadata;
  }

  /**
   * writes the embedded table record
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void write(E embeddedTableRec) throws IOException, InterruptedException {
    internalWriter.write(embeddedTableRec);
  }

  /**
   * This method provides the current approximate size in memory.
   *
   */
  public long getCurrentSize() {
    return internalWriter.getCurrentSize();
  }
}
