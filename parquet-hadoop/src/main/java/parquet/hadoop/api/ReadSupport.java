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
package parquet.hadoop.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * Abstraction used by the {@link parquet.hadoop.ParquetInputFormat} to materialize records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
abstract public class ReadSupport<T> {

  /**
   * configuration key for a parquet read projection schema
   */
	public static final String PARQUET_READ_SCHEMA = "parquet.read.schema";

  /**
   * attempts to validate and construct a {@link MessageType} from a read projection schema
   *
   * @param fileMessageType         the typed schema of the source
   * @param partialReadSchemaString the requested projection schema
   * @return the typed schema that should be used to read
   */
  public static MessageType getSchemaForRead(MessageType fileMessageType, String partialReadSchemaString) {
    if (partialReadSchemaString == null)
      return fileMessageType;
    MessageType requestedMessageType = MessageTypeParser.parseMessageType(partialReadSchemaString);
    return getSchemaForRead(fileMessageType, requestedMessageType);
  }

  public static MessageType getSchemaForRead(MessageType fileMessageType, MessageType projectedMessageType) {
    fileMessageType.checkContains(projectedMessageType);
    return projectedMessageType;
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param configuration    the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema       the schema of the file
   * @return the readContext that defines how to read the file
   *
   * @deprecated override {@link ReadSupport#init(InitContext)} instead
   */
  @Deprecated
  public ReadContext init(
          Configuration configuration,
          Map<String, String> keyValueMetaData,
          MessageType fileSchema) {
    throw new UnsupportedOperationException("Override init(InitContext)");
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)} in the front end
   *
   * @param context the initialisation context
   * @return the readContext that defines how to read the file
   */
  public ReadContext init(InitContext context) {
    return init(context.getConfiguration(), context.getMergedKeyValueMetaData(), context.getFileSchema());
  }

  /**
   * called in {@link org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)} in the back end
   * the returned RecordMaterializer will materialize the records and add them to the destination
   *
   * @param configuration    the job configuration
   * @param keyValueMetaData the app specific metadata from the file
   * @param fileSchema       the schema of the file
   * @param readContext      returned by the init method
   * @return the recordMaterializer that will materialize the records
   */
  abstract public RecordMaterializer<T> prepareForRead(
          Configuration configuration,
          Map<String, String> keyValueMetaData,
          MessageType fileSchema,
          ReadContext readContext);

  /**
   * information to read the file
   *
   * @author Julien Le Dem
   *
   */
  public static final class ReadContext {
    private final MessageType requestedSchema;
    private final Map<String, String> readSupportMetadata;
    private final List<EmbeddedTableSchema> embeddedTableSchemaList;
    private final boolean useBatchedRead;

    /**
     * @param requestedSchema the schema requested by the user. Can not be null.
     */
    public ReadContext(MessageType requestedSchema) {
      this(requestedSchema, null, null, false);
    }

    /**
     * @param requestedSchema the schema requested by the user. Can not be null.
     * @param readSupportMetadata metadata specific to the ReadSupport implementation. Will be available in the prepareForRead phase.
     */
    public ReadContext(MessageType requestedSchema, Map<String, String> readSupportMetadata) {
      this(requestedSchema, readSupportMetadata, null, false);
    }

    public ReadContext(MessageType requestedSchema,
                       Map<String,String> readSupportMetadata,
                       List<EmbeddedTableSchema> embeddedTableSchemaList) {
      this(requestedSchema, readSupportMetadata, embeddedTableSchemaList, false);
    }

    public ReadContext(MessageType requestedSchema,
                       Map<String,String> readSupportMetadata,
                       List<EmbeddedTableSchema> embeddedTableSchemaList,
                       boolean useBatchedRead) {
      super();
      if (requestedSchema == null) {
        throw new NullPointerException("requestedSchema");
      }
      this.requestedSchema = requestedSchema;
      this.readSupportMetadata = readSupportMetadata;
      this.embeddedTableSchemaList = (embeddedTableSchemaList == null) ? Collections.EMPTY_LIST : embeddedTableSchemaList;
      this.useBatchedRead = useBatchedRead;
    }

    /**
     * @return the schema of the file
     */
    public MessageType getRequestedSchema() {
      return requestedSchema;
    }

    /**
     * @return metadata specific to the ReadSupport implementation
     */
    public Map<String, String> getReadSupportMetadata() {
      return readSupportMetadata;
    }

    /**
     * @return Details of embedded tables that needs to be read.
     */
    public List<EmbeddedTableSchema> getEmbeddedTableSchemaList() {
      return embeddedTableSchemaList;
    }

    /**
     * @return returns true if parquet should be run in batched-read mode
     */
    public boolean useBatchedRead() {
      return useBatchedRead;
    }
  }

  /**
   * Holds details about the embedded table, and enables the InternalParquetRecordReader to read up
   * the embedded table.
   */
  public static final class EmbeddedTableSchema {
    private final long footerPosition;
    private final String footerPositionKey;
    private final MessageType requestedSchema;

    public EmbeddedTableSchema(long footerPosition, MessageType requestedSchema) {
      this(footerPosition, null, requestedSchema);
    }

    public EmbeddedTableSchema(String footerPositionKey, MessageType requestedSchema) {
      this(0L, footerPositionKey, requestedSchema);
    }

    private EmbeddedTableSchema(long footerPosition, String footerPositionKey, MessageType requestedSchema) {
      this.footerPosition = footerPosition;
      this.footerPositionKey = footerPositionKey;
      this.requestedSchema = requestedSchema;
    }

    /**
     * @return File offset at which the parquet footer for the embedded table could be found.
     */
    public long getFooterPosition() {
      return footerPosition;
    }

    /**
     * @return Key to file metadata entry that contains
     * the file offset at which the parquet footer for the embedded table could be found.
     */
    public String getFooterPositionKey() {
      return footerPositionKey;
    }

    /**
     * @return Projection list to be used while reading the embedded table.
     */
    public MessageType getRequestedSchema() {
      return requestedSchema;
    }
  }
}
