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
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by abennett on 8/7/15.
 */
public class TestEmbeddedTablePageReadStore {

    @Test
    public void testDelegation() {
        PageReadStore mainStore = mock(PageReadStore.class);
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        PageReader pageReader = mock(PageReader.class);
        when(mainStore.getPageReader(column)).thenReturn(pageReader);
        when(mainStore.getRowCount()).thenReturn(10L);
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mainStore);
        assertEquals(pageReadStore.getPageReader(column), pageReader);
        assertEquals(pageReadStore.getRowCount(), 10L);
    }

    @Test(expected = NullPointerException.class)
    public void testAddNullSchema() {
        PageReadStore mainStore = mock(PageReadStore.class);
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mainStore);
        pageReadStore.addPageReadStore(null, mock(PageReadStore.class));
    }

    @Test(expected = NullPointerException.class)
    public void testAddNullEmbeddedStore() {
        PageReadStore mainStore = mock(PageReadStore.class);
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mainStore);
        pageReadStore.addPageReadStore("test", null);
    }

    @Test
    public void testEmbeddedStore() {
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mock(PageReadStore.class));
        PageReadStore embeddedStore = mock(PageReadStore.class);
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        PageReader pageReader = mock(PageReader.class);
        when(embeddedStore.getPageReader(column)).thenReturn(pageReader);
        pageReadStore.addPageReadStore("test", embeddedStore);
        assertEquals(pageReadStore.getPageReadStore("test"), embeddedStore);
        assertEquals(pageReadStore.getPageReader("test", column), pageReader);
        assertEquals(pageReadStore.getPageReadStore("test").getPageReader(column), pageReader);
    }

}
