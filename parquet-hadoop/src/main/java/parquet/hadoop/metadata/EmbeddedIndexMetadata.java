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
package parquet.hadoop.metadata;

import java.util.*;

/**
 * Metadata for the Embedded Index content stored in a parquet file.
 * <p/>
 * Created by Jaspreet Singh
 */
public class EmbeddedIndexMetadata implements EmbeddedTableMetadata {
  private String embeddedIndexName;
  private String colNameOfIndexedCol;
  private String embeddedIndexTableName;
  private long embeddedIndexFooterPos = -1;

  /**
   * @param embeddedIndexName      name or type of the embedded index.
   * @param colNameOfIndexedCol    name of the data column which is indexed.
   * @param embeddedIndexFooterPos footer position of the footer of embedded index.
   * @param embeddedIndexTableName table name for the embedded index.
   */
  public EmbeddedIndexMetadata(String embeddedIndexName, String colNameOfIndexedCol,
                               long embeddedIndexFooterPos, String embeddedIndexTableName) {
    this.embeddedIndexName = embeddedIndexName;
    this.colNameOfIndexedCol = colNameOfIndexedCol;
    this.embeddedIndexFooterPos = embeddedIndexFooterPos;
    this.embeddedIndexTableName = embeddedIndexTableName;
  }

  /**
   * @param embeddedIndexName      name or type of the embedded index.
   * @param colNameOfIndexedCol    name of the data column which is indexed.
   * @param embeddedIndexTableName table name for the embedded index.
   */
  public EmbeddedIndexMetadata(String embeddedIndexName, String colNameOfIndexedCol,
                               String embeddedIndexTableName) {
    this.embeddedIndexName = embeddedIndexName;
    this.colNameOfIndexedCol = colNameOfIndexedCol;
    this.embeddedIndexTableName = embeddedIndexTableName;
  }

  /**
   * get the embedded index metadata for a given index and data column.
   *
   * @param fileKeyValueMetadata key value metadata from file metadata
   * @param indexName            name or type of the index
   * @param dataColumnName       name of the data column which is indexed
   * @return null if no index found.
   */
  public static EmbeddedIndexMetadata getEmbeddedIndexMetadata(
      Map<String, String> fileKeyValueMetadata, String indexName, String dataColumnName) {

    EmbeddedIndexMetadata embIndexMetadata = null;
    String fposKey = genFooterPosKey(dataColumnName, indexName);
    String tableNameKey = genTableNameKey(dataColumnName, indexName);

    if (fileKeyValueMetadata.containsKey(fposKey)) {
      Long fpos = Long.valueOf(fileKeyValueMetadata.get(fposKey));
      String embIndexTableName = fileKeyValueMetadata.get(tableNameKey);

      embIndexMetadata = new EmbeddedIndexMetadata(indexName, dataColumnName, fpos,
          embIndexTableName);
    }

    return embIndexMetadata;
  }

  public static String genFooterPosKey(String colNameOfIndexedCol, String embIndexName) {
    return genPartialKey(colNameOfIndexedCol) + embIndexName + ".FOOTERPOS";
  }

  public static String genTableNameKey(String colNameOfIndexedCol, String embIndexName) {

    return genPartialKey(colNameOfIndexedCol) + embIndexName + ".TABLENAME";
  }

  private static String genPartialKey(String colNameOfIndexedCol) {
    return "COLNAME." + colNameOfIndexedCol + ".INDEX.";
  }

  /**
   * get embedded index metadata for all indexes on a given data column.
   *
   * @param fileKeyValueMetadata
   * @param colName
   * @return
   */
  public static List<EmbeddedIndexMetadata> getEmbeddedIndexMetadataForColumn(
      Map<String, String> fileKeyValueMetadata, String colName) {

    List<EmbeddedIndexMetadata> embIndexList = new ArrayList<EmbeddedIndexMetadata>();
    Set<String> embIndexNames = new HashSet<String>();

    for (String key : fileKeyValueMetadata.keySet()) {
      String partialKey = genPartialKey(colName);
      if (key.startsWith(partialKey)) {

        int embIndexNameEndOffset = key.indexOf(".", partialKey.length());
        String embIndexName = key.substring(partialKey.length(), embIndexNameEndOffset);

        if (!embIndexNames.contains(embIndexName)) {
          embIndexNames.add(embIndexName);

          EmbeddedIndexMetadata embIM = getEmbeddedIndexMetadata(fileKeyValueMetadata, embIndexName,
              colName);
          if (embIM != null) embIndexList.add(embIM);
        }
      }
    }

    return embIndexList;
  }

  /**
   * Adds the current embedded index metadata to the input key value metadata of parquet file
   *
   * @param fileKeyValMetadata key value metadata map to which the embedded index metadata
   *                           is to be added
   */
  @Override
  public void addEmbeddedTableMetadataToFileMetadata(Map<String, String> fileKeyValMetadata) {
    fileKeyValMetadata.put(genMyFooterPosKey(), String.valueOf(embeddedIndexFooterPos));
    fileKeyValMetadata.put(genMyTableNameKey(), embeddedIndexTableName);
  }

  @Override
  public void setEmbeddedTableFooterPosition(long footerPosition) {
    this.embeddedIndexFooterPos = footerPosition;
  }

  private String genMyFooterPosKey() {
    return genFooterPosKey(colNameOfIndexedCol, embeddedIndexName);
  }

  private String genMyTableNameKey() {
    return genTableNameKey(colNameOfIndexedCol, embeddedIndexName);
  }

  public String getEmbeddedIndexName() {
    return embeddedIndexName;
  }

  public String getColNameOfIndexedCol() {
    return colNameOfIndexedCol;
  }

  public long getEmbeddedIndexFooterPos() {
    return embeddedIndexFooterPos;
  }

  public String getEmbeddedIndexTableName() {
    return embeddedIndexTableName;
  }
}
