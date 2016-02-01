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
package parquet.column.iotas;

import org.junit.Test;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.column.page.PageWriter;
import parquet.column.page.mem.MemPageStore;
import parquet.column.statistics.IntStatistics;
import parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN;

/**
 * Created by abennett on 8/7/15.
 */
public class TestEmbeddedTableColumnReaderFactory {

    @Test
    public void test() throws IOException {
        MemPageStore memPageStore = new MemPageStore(10);
        String[] path = {"a"};
        ColumnDescriptor col = new ColumnDescriptor(path , PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
        IntStatistics stats = new IntStatistics();
        PageWriter pageWriter = memPageStore.getPageWriter(col);
        pageWriter.writePage(BytesInput.from(new byte[100]), 10, stats, BIT_PACKED, BIT_PACKED, PLAIN);
        PageReader pageReader = memPageStore.getPageReader(col);

        PageReadStore embeddedPageReadStore = mock(PageReadStore.class);
        when(embeddedPageReadStore.getPageReader(col)).thenReturn(pageReader);
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mock(PageReadStore.class));
        pageReadStore.addPageReadStore("test", embeddedPageReadStore);
        EmbeddedTableColumnReaderFactory factory = new EmbeddedTableColumnReaderFactory(pageReadStore, false);
        ColumnReader reader = factory.getColumnReader("test", col);

        assertNotNull(reader);
        assertEquals(reader.getTotalValueCount(), 10);
    }
}
