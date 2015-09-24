package parquet.filter2.predicate.iotas.index;

import org.junit.Test;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.iotas.SuffixArrayPredicate;
import parquet.filter2.predicate.iotas.WordEndsWithPredicate;
import parquet.filter2.predicate.iotas.WordMatchesPredicate;
import parquet.filter2.predicate.iotas.WordStartsWithPredicate;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static parquet.filter2.predicate.iotas.index.TestSuffixArrayUtils.deltaPack;
import static parquet.filter2.predicate.iotas.index.TestSuffixArrayUtils.binaryPackBoolean;

/**
 * Created by abennett on 16/7/15.
 */
public class TestWordPredicates {



    @Test
    public void testWordStartsWith() throws IOException {
        WordStartsWithPredicate predicate = new WordStartsWithPredicate("suffixArrayIndexTable", "doc_value", "desk");
        predicate.init(buildFactory());

        assertFalse(predicate.canDrop(mock(Statistics.class)));
        assertFalse(predicate.inverseCanDrop(mock(Statistics.class)));

        assertFalse(predicate.keep(0));
        assertFalse(predicate.keep(1));
        assertFalse(predicate.keep(2));
        assertFalse(predicate.keep(3));
        assertFalse(predicate.keep(4));
        assertFalse(predicate.keep(5));
        assertTrue(predicate.keep(6));
        assertFalse(predicate.keep(7));
        assertFalse(predicate.keep(8));
        assertFalse(predicate.keep(9));
        assertFalse(predicate.keep(10));
        assertTrue(predicate.keep(11));
        assertFalse(predicate.keep(12));
        assertFalse(predicate.keep(13));
        assertFalse(predicate.keep(14));
        assertFalse(predicate.keep(15));
        assertFalse(predicate.keep(16));
        assertFalse(predicate.keep(17));
        assertFalse(predicate.keep(18));
    }


    @Test
    public void testWordEndsWith() throws IOException {
        WordEndsWithPredicate predicate = new WordEndsWithPredicate("suffixArrayIndexTable", "doc_value", "desk");
        predicate.init(buildFactory());

        assertFalse(predicate.canDrop(mock(Statistics.class)));
        assertFalse(predicate.inverseCanDrop(mock(Statistics.class)));

        assertFalse(predicate.keep(0));
        assertFalse(predicate.keep(1));
        assertFalse(predicate.keep(2));
        assertFalse(predicate.keep(3));
        assertFalse(predicate.keep(4));
        assertTrue(predicate.keep(5));
        assertTrue(predicate.keep(6));
        assertTrue(predicate.keep(7));
        assertFalse(predicate.keep(8));
        assertFalse(predicate.keep(9));
        assertFalse(predicate.keep(10));
        assertFalse(predicate.keep(11));
        assertFalse(predicate.keep(12));
        assertFalse(predicate.keep(13));
        assertFalse(predicate.keep(14));
        assertFalse(predicate.keep(15));
        assertFalse(predicate.keep(16));
        assertFalse(predicate.keep(17));
        assertFalse(predicate.keep(18));
    }

    @Test
    public void testWordMatches() throws IOException {
        WordMatchesPredicate predicate = new WordMatchesPredicate("suffixArrayIndexTable", "doc_value", "desk");
        predicate.init(buildFactory());

        assertFalse(predicate.canDrop(mock(Statistics.class)));
        assertFalse(predicate.inverseCanDrop(mock(Statistics.class)));

        assertFalse(predicate.keep(0));
        assertFalse(predicate.keep(1));
        assertFalse(predicate.keep(2));
        assertFalse(predicate.keep(3));
        assertFalse(predicate.keep(4));
        assertFalse(predicate.keep(5));
        assertTrue(predicate.keep(6));
        assertFalse(predicate.keep(7));
        assertFalse(predicate.keep(8));
        assertFalse(predicate.keep(9));
        assertFalse(predicate.keep(10));
        assertFalse(predicate.keep(11));
        assertFalse(predicate.keep(12));
        assertFalse(predicate.keep(13));
        assertFalse(predicate.keep(14));
        assertFalse(predicate.keep(15));
        assertFalse(predicate.keep(16));
        assertFalse(predicate.keep(17));
        assertFalse(predicate.keep(18));
    }


    private EmbeddedTableColumnReaderFactory buildFactory() throws IOException{
        BatchColumnReader termReader = mock(BatchColumnReader.class);
        when(termReader.getMaxBatchSize()).thenReturn(10);
        when(termReader.getTotalValueCount()).thenReturn(4L);
        when(termReader.getBinary()).thenReturn(
                Binary.fromString("desert"),
                Binary.fromString("desk"),
                Binary.fromString("desktop"),
                Binary.fromString("helpdesk"));

        BatchColumnReader idReader = mock(BatchColumnReader.class);
        when(idReader.getMaxBatchSize()).thenReturn(10);
        when(idReader.getTotalValueCount()).thenReturn(4L);
        when(idReader.getBinary()).thenReturn(
                deltaPack(0),
                deltaPack(5),
                deltaPack(10),
                deltaPack(15));

        BatchColumnReader flagReader = mock(BatchColumnReader.class);
        when(flagReader.getMaxBatchSize()).thenReturn(10);
        when(flagReader.getTotalValueCount()).thenReturn(4L);
        when(flagReader.getBinary()).thenReturn(
                binaryPackBoolean(false, true, false),
                binaryPackBoolean(false, true, false),
                binaryPackBoolean(false, true, false),
                binaryPackBoolean(false, true, false));

        String[] colPath = {SuffixArrayPredicate.TERM_COLUMN};
        ColumnDescriptor termColumn = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);

        String[] idPath = {SuffixArrayPredicate.DOC_IDS_COLUMN};
        ColumnDescriptor idColumn = new ColumnDescriptor(idPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);

        String[] flagPath = {SuffixArrayPredicate.STARTS_WITH_FLAG_COLUMN};
        ColumnDescriptor flagColumn = new ColumnDescriptor(flagPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);

        EmbeddedTableColumnReaderFactory factory = mock(EmbeddedTableColumnReaderFactory.class);
        when(factory.getColumnReader("suffixArrayIndexTable", termColumn)).thenReturn(termReader);
        when(factory.getColumnReader("suffixArrayIndexTable", idColumn)).thenReturn(idReader);
        when(factory.getColumnReader("suffixArrayIndexTable", flagColumn)).thenReturn(flagReader);

        return factory;
    }

}
