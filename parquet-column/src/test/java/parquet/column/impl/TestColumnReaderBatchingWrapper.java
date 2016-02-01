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

import org.junit.Test;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnReader;

import static org.mockito.Mockito.*;

/**
 * Created by abennett on 23/7/15.
 */
public class TestColumnReaderBatchingWrapper {

    @Test
    public void test() {
        BatchColumnReader batchColumnReader = mock(BatchColumnReader.class);
        when(batchColumnReader.getMaxBatchSize()).thenReturn(5);
        when(batchColumnReader.getTotalValueCount()).thenReturn(12L);
        when(batchColumnReader.getInteger()).thenReturn(1);
        when(batchColumnReader.getRemainingPageValueCount()).thenReturn(12, 7, 2);
        ColumnReader columnReader = new ColumnReaderBatchingWrapper(batchColumnReader);

        assert(columnReader.getTotalValueCount() == 12);
        assert(columnReader.getRemainingValueCount() == 12);

        for(int i =0; i< 6; i++) {
            assert(columnReader.getInteger() == 1);
            columnReader.consume();
        }
        assert(columnReader.getTotalValueCount() == 12);
        assert(columnReader.getRemainingValueCount() == 6);
        verify(batchColumnReader, times(2)).writeCurrentBatchToConverter(5);

        for(int i =0; i< 6; i++) {
            assert(columnReader.getInteger() == 1);
            columnReader.consume();
        }
        assert(columnReader.getTotalValueCount() == 12);
        assert(columnReader.getRemainingValueCount() == 0);
        verify(batchColumnReader, times(1)).writeCurrentBatchToConverter(2);

    }
}
