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

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Jaspreet Singh
 */
public class TestEmbIndexMetadata {

  @Test
  public void testSingleReadOnlyFooterPosInMetadata() {
    Map<String, String> fileKVMetadata = new HashMap<String, String>();
    String indexName = "suffixArray";
    String indexedCol = "data";
    long footerPos = 1000L;

    fileKVMetadata.put(EmbeddedIndexMetadata.genFooterPosKey(indexedCol, indexName),
        String.valueOf(footerPos)
    );

    EmbeddedIndexMetadata retval = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(fileKVMetadata, indexName,
        indexedCol);

    assertNotEquals(null, retval);
    assertEquals(null, retval.getEmbeddedIndexTableName());

  }

  @Test
  public void testSingleIndexRead() {

    Map<String, String> fileKVMetadata = new HashMap<String, String>();
    String indexName = "suffixArray";
    String indexedCol = "data";
    long footerPos = 1000L;
    String tableName = "termTable";

    fileKVMetadata.put(EmbeddedIndexMetadata.genTableNameKey(indexedCol, indexName), tableName);

    EmbeddedIndexMetadata ret1 = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(fileKVMetadata, indexName,
        indexedCol);

    assertEquals(null, ret1);

    fileKVMetadata.put(EmbeddedIndexMetadata.genFooterPosKey(indexedCol, indexName),
        String.valueOf(footerPos)
    );

    EmbeddedIndexMetadata retEmbIndex1 = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(fileKVMetadata, "fuzzy",
        "data");

    assertEquals(null, retEmbIndex1);

    EmbeddedIndexMetadata retEmbIndex2 = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(fileKVMetadata, indexName,
        indexedCol);

    assertNotEquals(null, retEmbIndex2);

    assertEquals(retEmbIndex2.getEmbeddedIndexName(), indexName);
    assertEquals(retEmbIndex2.getColNameOfIndexedCol(), indexedCol);
    assertEquals(retEmbIndex2.getEmbeddedIndexFooterPos(), footerPos);
    assertEquals(retEmbIndex2.getEmbeddedIndexTableName(), tableName);
  }

  @Test
  public void testAddEmbIndexMetadata() {
    Map<String, String> fileKVMetadata = new HashMap<String, String>();

    EmbeddedIndexMetadata embIndex1 = new EmbeddedIndexMetadata("suffixArray", "data", 1000L, "termTable");

    embIndex1.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);

    EmbeddedIndexMetadata retEmbIndex = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(fileKVMetadata,
        embIndex1.getEmbeddedIndexName(), embIndex1.getColNameOfIndexedCol());

    assertTrue(isEqual(embIndex1, retEmbIndex));
  }

  @Test
  public void testMultiReadWithSingleIndex() {
    Map<String, String> fileKVMetadata = new HashMap<String, String>();

    EmbeddedIndexMetadata embIndex1 = new EmbeddedIndexMetadata("suffixArray", "data", 1000L, "termTable");

    embIndex1.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);

    List<EmbeddedIndexMetadata> retEmbIndexList = EmbeddedIndexMetadata.getEmbeddedIndexMetadataForColumn(fileKVMetadata,
        embIndex1.getColNameOfIndexedCol());

    assertEquals(retEmbIndexList.size(), 1);

    assertTrue(isEqual(embIndex1, retEmbIndexList.get(0)));
  }

  @Test
  public void testMultiReadWithMultipleIndex() {
    Map<String, String> fileKVMetadata = new HashMap<String, String>();

    EmbeddedIndexMetadata embIndex1 = new EmbeddedIndexMetadata("suffixArray", "data", 1000L, "termTable");
    EmbeddedIndexMetadata embIndex2 = new EmbeddedIndexMetadata("fuzzy", "data", 1000L, "termTable");
    EmbeddedIndexMetadata embIndex3 = new EmbeddedIndexMetadata("suffixArray", "values", 1000L, "termTable");

    embIndex1.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);
    embIndex2.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);
    embIndex3.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);

    List<EmbeddedIndexMetadata> retEmbIndexList = EmbeddedIndexMetadata.getEmbeddedIndexMetadataForColumn(fileKVMetadata,
        embIndex1.getColNameOfIndexedCol());

    assertEquals(retEmbIndexList.size(), 2);

    assertTrue(isEqual(embIndex1, retEmbIndexList.get(0)) ||
            isEqual(embIndex1, retEmbIndexList.get(1))
    );
  }

  @Test
  public void testMultiReadWithIncompleteMetadata() {
    Map<String, String> fileKVMetadata = new HashMap<String, String>();

    EmbeddedIndexMetadata embIndex1 = new EmbeddedIndexMetadata("suffixArray", "data", 1000L, "termTable");
    EmbeddedIndexMetadata embIndex2 = new EmbeddedIndexMetadata("fuzzy", "data", 1000L, "termTable");
    EmbeddedIndexMetadata embIndex3 = new EmbeddedIndexMetadata("suffixArray", "values", 1000L, "termTable");

    embIndex1.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);
    embIndex2.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);
    embIndex3.addEmbeddedTableMetadataToFileMetadata(fileKVMetadata);

    fileKVMetadata.put(EmbeddedIndexMetadata.genTableNameKey("data", "indexWithoutFooterPos"),
        "noFooter");

    List<EmbeddedIndexMetadata> retEmbIndexList = EmbeddedIndexMetadata.getEmbeddedIndexMetadataForColumn(fileKVMetadata,
        embIndex1.getColNameOfIndexedCol());

    // The index for which footer position is not present, embedded metadata is not returned.
    assertEquals(2, retEmbIndexList.size());

    assertTrue(isEqual(embIndex1, retEmbIndexList.get(0)) ||
            isEqual(embIndex1, retEmbIndexList.get(1))
    );
  }


  private boolean isEqual(EmbeddedIndexMetadata emb1, EmbeddedIndexMetadata emb2) {
    return (emb1.getEmbeddedIndexName().equals(emb2.getEmbeddedIndexName()) &&
        emb1.getColNameOfIndexedCol().equals(emb2.getColNameOfIndexedCol()) &&
        emb1.getEmbeddedIndexFooterPos() == emb2.getEmbeddedIndexFooterPos() &&
        emb1.getEmbeddedIndexTableName().equals(emb1.getEmbeddedIndexTableName()));
  }
}
