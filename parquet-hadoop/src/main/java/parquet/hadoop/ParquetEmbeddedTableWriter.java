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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.iotas.ParquetEmbeddedTableFileWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.EmbeddedTableMetadata;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes records and embedded table records to a parquet file.
 *
 * Created by Jaspreet Singh
 * @author pdas
 */
public class ParquetEmbeddedTableWriter<T> implements Closeable {

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;
  public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME =
      CompressionCodecName.UNCOMPRESSED;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
  public static final ParquetProperties.WriterVersion DEFAULT_WRITER_VERSION =
      ParquetProperties.WriterVersion.PARQUET_1_0;

  private final IotasInternalRecordWriter<T> recWriter;
  private List<ParquetEmbeddedTableRecordWriter<?>> embeddedTableWriters =
      new ArrayList<ParquetEmbeddedTableRecordWriter<?>>();
  private ParquetEmbeddedTableFileWriter fileWriter;
  private Configuration conf;
  private long blockSize;
  private int pageSize;
  private int dictionaryPageSize;
  private CodecFactory.BytesCompressor compressor;
  private ParquetProperties.WriterVersion writerVersion;

  public ParquetEmbeddedTableWriter(
      Path file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      long blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf) throws IOException {

    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    fileWriter = new ParquetEmbeddedTableFileWriter(conf, schema, file, mode);
    this.recWriter = initialize(fileWriter, writeSupport, schema, writeContext.getExtraMetaData(),
        compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary,
        validating, writerVersion, conf);
  }

  public ParquetEmbeddedTableWriter(
      ParquetEmbeddedTableFileWriter fileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetadata,
      CompressionCodecName compressionCodecName,
      long blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf) throws IOException {

    this.recWriter = initialize(fileWriter, writeSupport, schema, extraMetadata,
        compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary,
        validating, writerVersion, conf);
  }

  private IotasInternalRecordWriter<T> initialize(
      ParquetEmbeddedTableFileWriter fileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetadata,
      CompressionCodecName compressionCodecName,
      long blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      ParquetProperties.WriterVersion writerVersion,
      Configuration conf) throws IOException {

    this.conf = conf;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.dictionaryPageSize = dictionaryPageSize;
    this.writerVersion = writerVersion;
    this.fileWriter = fileWriter;
    fileWriter.start(); // make it ready to receive flushed records

    CodecFactory codecFactory = new CodecFactory(conf);
    compressor =  codecFactory.getCompressor(compressionCodecName, 0);
    return new IotasInternalRecordWriter<T>(
        fileWriter,
        writeSupport,
        schema,
        extraMetadata,
        blockSize,
        pageSize,
        compressor,
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion);
  }
 
  /**
   * Adds an embedded table record writer. Use write on this returned embedded table writer to write
   * embedded table records.
   *
   * @param embeddedTableWriteSupport         write support for embedded table record
   * @param enableDictionary          whether dictionary is to be enabled for this embedded table.
   * @param validating                whether validation is to be enabled for this embedded table.
   * @param embeddedTableMetadata     metadata for the embedded table. written to parquet file
   *                                  metadata
   */
  public <I> ParquetEmbeddedTableRecordWriter<I> addEmbeddedTableRecordWriter(
      WriteSupport<I> embeddedTableWriteSupport,
      boolean enableDictionary,
      boolean validating,
      EmbeddedTableMetadata embeddedTableMetadata) {

    WriteSupport.WriteContext embTableWriteContext = embeddedTableWriteSupport.init(conf);
    MessageType embTableSchema = embTableWriteContext.getSchema();

    ParquetEmbeddedTableRecordWriter<I> embeddedTableRecWriter =
        new ParquetEmbeddedTableRecordWriter<I>(
            fileWriter,
            embeddedTableWriteSupport,
            embTableSchema,
            embTableWriteContext.getExtraMetaData(),
            blockSize,
            pageSize,
            compressor,
            dictionaryPageSize,
            enableDictionary,
            validating,
            writerVersion,
            embeddedTableMetadata);


    embeddedTableWriters.add(embeddedTableRecWriter);
    return embeddedTableRecWriter;
  }

  /**
   * writes records to parquet file.
   *
   * @param record
   * @throws IOException
   */
  public void write(T record) throws IOException {
    try {
      recWriter.write(record);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * This method provides the current approximate size in memory.
   *
   */
  public long getCurrentSize() {
    long curSize = 0;
    for(ParquetEmbeddedTableRecordWriter<?> embeddedTableWriter : embeddedTableWriters) {
      curSize += embeddedTableWriter.getCurrentSize();
    }

    curSize += recWriter.getCurrentSize();

    return curSize;
  }

  /**
   * Flushes the records including the embedded records to parquet file and closes the writer.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {

    Map<String, String> embeddedTablesMetadata = new HashMap<String, String>();
    try {

      for(ParquetEmbeddedTableRecordWriter<?> embeddedTableWriter : embeddedTableWriters) {
        embeddedTableWriter.close();
        embeddedTableWriter.getEmbeddedTableMetadata()
            .addEmbeddedTableMetadataToFileMetadata(embeddedTablesMetadata);
      }

      recWriter.close(embeddedTablesMetadata);

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Flushes the records including the embedded records to parquet file as if close
   * was invoked on the writer, but does not close the writer. This call is useful
   * for taking a consistent snapshot of Parquet file content, but more records can
   * be appended after this call. The subsequent flushAsIfClose() will contain all
   * the previous records and the newly added ones until close is called.
   *
   * This call allows incremental addition of records to an in-memory cached parquet
   * format file efficiently, as the encoding of all the previous records can be
   * avoided and incremental encoding cost is incurred.
   *
   * @throws IOException
   */
  public void flushAsIfClose() throws IOException {

    Map<String, String> embeddedTablesMetadata = new HashMap<String, String>();
    try {

      for(ParquetEmbeddedTableRecordWriter<?> embeddedTableWriter : embeddedTableWriters) {
        embeddedTableWriter.flushAsIfClose();
        embeddedTableWriter.getEmbeddedTableMetadata()
            .addEmbeddedTableMetadataToFileMetadata(embeddedTablesMetadata);
      }

      recWriter.flushAsIfClose(embeddedTablesMetadata);

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
