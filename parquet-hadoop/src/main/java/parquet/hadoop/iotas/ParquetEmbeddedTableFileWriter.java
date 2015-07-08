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
package parquet.hadoop.iotas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.Log;
import parquet.Version;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Extends ParquetFileWriter to add Embedded Table content to a parquet file.
 * Multiple tables may be embedded in a single parquet file. The embedded tables may be used
 * to store index or other lookup content for the main table data stored in the parquet file.
 *
 * Created by Jaspreet Singh.
 */
public class ParquetEmbeddedTableFileWriter extends ParquetFileWriter {
  private static final Log LOG = Log.getLog(ParquetEmbeddedTableFileWriter.class);


  // Each Embedded Table block is written with its own Footer. A Parquet File may have
  // multiple Embedded Tables. This list is cleared after each embedded table is written.
  // In the current model, it is expected that usually the list will have a single element.
  List<BlockMetaData> embeddedTableBlocks = new ArrayList<BlockMetaData>();

  public ParquetEmbeddedTableFileWriter(Configuration configuration,
                                        MessageType schema,
                                        Path file,
                                        Mode mode) throws IOException {
    super(configuration, schema, file, mode);
  }

  /**
   * puts the file writer in a state to write the embedded table block.
   *
   * @param embeddedTableRecCount
   * @throws IOException
   */
  public void startEmbeddedTableBlock(long embeddedTableRecCount) throws IOException {
    startBlock(embeddedTableRecCount);
  }

  /**
   * updates the embedded table blocks list for embedded table metadata.
   *
   * @throws IOException
   */
  public void endEmbeddedTableBlock() throws IOException {
    setStateEndBlock();

    if (DEBUG) LOG.debug(out.getPos() + "{} : end embedded table block");

    currentBlock.setRowCount(currentRecordCount);
    embeddedTableBlocks.add(currentBlock);
    currentBlock = null;
  }

  /**
   * writes the footer for embedded Table.
   *
   * @param embeddedTableSchema
   * @param extraMetaData
   * @throws IOException
   */
  public void writeEmbeddedTableFooter(MessageType embeddedTableSchema,
                                       Map<String, String> extraMetaData) throws IOException {

    if (!isStateStarted()) {
      throw new IOException("File Writer cannot write Embedded Table in State : " + state);
    }

    if (DEBUG) LOG.debug(out.getPos() + ": write embedded table footer");

    ParquetMetadata footer = new ParquetMetadata(
        new FileMetaData(embeddedTableSchema, extraMetaData, Version.FULL_VERSION),
        embeddedTableBlocks);
    serializeFooter(footer, out);
    embeddedTableBlocks.clear();
  }



}
