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
