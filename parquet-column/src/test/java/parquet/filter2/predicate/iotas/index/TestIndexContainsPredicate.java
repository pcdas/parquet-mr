package parquet.filter2.predicate.iotas.index;

import org.junit.Test;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.iotas.IndexContainsPredicate;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static parquet.filter2.predicate.iotas.index.TestSuffixArrayUtils.deltaPack;

/**
 * Created by abennett on 16/7/15.
 */
public class TestIndexContainsPredicate {

    @Test
    public void test() throws IOException {
        BatchColumnReader termReader = mock(BatchColumnReader.class);
        when(termReader.getMaxBatchSize()).thenReturn(10);
        when(termReader.getTotalValueCount()).thenReturn(3L);
        when(termReader.getBinary()).thenReturn(
                Binary.fromString("abc"),
                Binary.fromString("base"),
                Binary.fromString("abc"));

        BatchColumnReader idReader = mock(BatchColumnReader.class);
        when(idReader.getMaxBatchSize()).thenReturn(10);
        when(idReader.getTotalValueCount()).thenReturn(3L);
        when(idReader.getBinary()).thenReturn(
                deltaPack(0),
                deltaPack(5),
                deltaPack(10));

        String[] colPath = {"term"};
        ColumnDescriptor termColumn = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);

        String[] idPath = {"doc_ids"};
        ColumnDescriptor idColumn = new ColumnDescriptor(idPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);

        EmbeddedTableColumnReaderFactory factory = mock(EmbeddedTableColumnReaderFactory.class);
        when(factory.getColumnReader("suffixArrayIndexTable", termColumn)).thenReturn(termReader);
        when(factory.getColumnReader("suffixArrayIndexTable", idColumn)).thenReturn(idReader);

        IndexContainsPredicate predicate = new IndexContainsPredicate("suffixArrayIndexTable", "doc_value", "base");
        predicate.init(factory);

        assertFalse(predicate.canDrop(mock(Statistics.class)));
        assertFalse(predicate.inverseCanDrop(mock(Statistics.class)));

        assertFalse(predicate.keep(0));
        assertFalse(predicate.keep(1));
        assertTrue(predicate.keep(5));
        assertTrue(predicate.keep(6));
        assertTrue(predicate.keep(7));
        assertFalse(predicate.keep(8));
        assertFalse(predicate.keep(10));
    }


}
