package parquet.filter2.predicate.iotas.index;

import org.junit.Test;
import parquet.column.ColumnReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.io.api.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by abennett on 15/7/15.
 */
public class TestSuffixArrayUtils {

    @Test
    public void testSearchTermStartsWith() {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(10L);
        when(reader.getBinary()).thenReturn(
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("bas"),
                Binary.fromString("base"),
                Binary.fromString("based"),
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("base"),
                Binary.fromString("abc"));
        //will not assume that the column is sorted
        long[] pos = SuffixArrayUtils.findTermStartsWithPos(reader, "base");
        assert(pos.length == 3);
        assertTrue(pos[0] == 4L);
        assertTrue(pos[1] == 5L);
        assertTrue(pos[2] == 8L);
    }

    @Test
    public void testSearchTermMatches() {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(10L);
        when(reader.getBinary()).thenReturn(
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("bas"),
                Binary.fromString("base"),
                Binary.fromString("based"),
                Binary.fromString("abc"),
                Binary.fromString("abc"),
                Binary.fromString("base"),
                Binary.fromString("abc"));
        //will not assume that the column is sorted
        long[] pos = SuffixArrayUtils.findTermMatchPos(reader, "base");
        assert(pos.length == 2);
        assertTrue(pos[0] == 4L);
        assertTrue(pos[1] == 8L);
    }

    @Test
    public void testSearchTermAbsent() {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(2L);
        when(reader.getBinary()).thenReturn(
                Binary.fromString("abc"),
                Binary.fromString("abc"));
        long[] pos = SuffixArrayUtils.findTermStartsWithPos(reader, "base");
        assertTrue(pos.length == 0);
    }

    @Test
    public void testUniqueIdList() throws IOException {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(10L);
        when(reader.getBinary()).thenReturn(deltaPack(0), deltaPacks(1, 9));
        long[] positions = new long[] { 0L, 3L, 4L, 9L};
        SortedIntIterator ids = SuffixArrayUtils.getUniqueIdList(reader, positions);
        List<Integer> results = new ArrayList<Integer>();
        while (ids.hasNext())
            results.add(ids.nextDedupe());
        assert(results.contains(0));
        assert(results.contains(1));
        assert(results.contains(2));
        assert(results.contains(3));
        assert(results.contains(4));
        assert(results.contains(5));
        assert(results.contains(6));
        assert(results.contains(9));
        assert(results.contains(10));
        assert(results.contains(11));
        assert(results.size() == 10);
    }

    @Test
    public void testFilteredUniqueIdList() throws IOException {
        ColumnReader idReader = mock(ColumnReader.class);
        when(idReader.getTotalValueCount()).thenReturn(3L);
        when(idReader.getBinary()).thenReturn(deltaPack(0), deltaPack(5), deltaPack(10));

        ColumnReader flagReader = mock(ColumnReader.class);
        when(flagReader.getTotalValueCount()).thenReturn(3L);
        when(flagReader.getBinary()).thenReturn(
                binaryPackBoolean(false, false, true),
                binaryPackBoolean(false, false, false),
                binaryPackBoolean(true, false, true));

        long[] positions = new long[] { 0L, 2L};
        SortedIntIterator ids = SuffixArrayUtils.getFilteredUniqueIdList(idReader, positions, flagReader);
        List<Integer> results = new ArrayList<Integer>();
        while (ids.hasNext())
            results.add(ids.nextDedupe());
        assert(results.contains(2));
        assert(results.contains(10));
        assert(results.contains(12));
        assert(results.size() == 3);
    }

    private Binary[] deltaPacks(int start, int size) throws IOException {
        Binary[] packs = new Binary[size];
        for(int i = 0; i < size; i++) {
            packs[i] = deltaPack(start + i);
        }
        return packs;
    }

    public static Binary deltaPack(int i) throws IOException {
        DeltaBinaryPackingValuesWriter writer = new DeltaBinaryPackingValuesWriter(100, 100);
        writer.writeInteger(i);
        writer.writeInteger(i + 1);
        writer.writeInteger(i + 2);
        return Binary.fromByteArray(writer.getBytes().toByteArray());
    }

    public static Binary binaryPackBoolean(boolean a, boolean b, boolean c) throws IOException {
        BooleanPlainValuesWriter writer = new BooleanPlainValuesWriter();
        writer.writeBoolean(a);
        writer.writeBoolean(b);
        writer.writeBoolean(c);
        return Binary.fromByteArray(writer.getBytes().toByteArray());
    }
}
