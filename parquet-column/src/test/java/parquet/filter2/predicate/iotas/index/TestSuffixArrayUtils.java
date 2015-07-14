package parquet.filter2.predicate.iotas.index;

import org.junit.Test;
import parquet.column.ColumnReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
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
    public void testSearchTerm() {
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
        List<Long> pos = SuffixArrayUtils.findTermStartsWithPos(reader, "base");
        assert(pos.size() == 2);
        assertTrue(pos.contains(4L));
        assertTrue(pos.contains(5L));
    }

    @Test
    public void testSearchTermAbsent() {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(2L);
        when(reader.getBinary()).thenReturn(
                Binary.fromString("abc"),
                Binary.fromString("abc"));
        List<Long> pos = SuffixArrayUtils.findTermStartsWithPos(reader, "base");
        assertTrue(pos.isEmpty());
    }

    @Test
    public void testUniqueIdList() throws IOException {
        ColumnReader reader = mock(ColumnReader.class);
        when(reader.getTotalValueCount()).thenReturn(10L);
        when(reader.getBinary()).thenReturn(deltaPack(0), deltaPacks(1, 9));
        List<Long> positions = new ArrayList<Long>();
        positions.add(0L);
        positions.add(3L);
        positions.add(4L);
        positions.add(9L);
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

    private Binary[] deltaPacks(int start, int size) throws IOException {
        Binary[] packs = new Binary[size];
        for(int i = 0; i < size; i++) {
            packs[i] = deltaPack(start + i);
        }
        return packs;
    }

    private Binary deltaPack(int i) throws IOException {
        DeltaBinaryPackingValuesWriter writer = new DeltaBinaryPackingValuesWriter(100, 100);
        writer.writeInteger(i);
        writer.writeInteger(i + 1);
        writer.writeInteger(i + 2);
        return Binary.fromByteArray(writer.getBytes().toByteArray());
    }
}
