package parquet.filter2.predicate.iotas.index;

import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.io.api.Binary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by abennett on 15/7/15.
 */
public class SuffixArrayUtils {

    private static final int[] EMPTY = new int[0];

    public static long[] findTermStartsWithPos(ColumnReader termReader, String term) {
        Binary termBin = Binary.fromString(term);
        int termLength = termBin.length();
        long total = termReader.getTotalValueCount();
        long read = 0;
        LongArrayList posList = new LongArrayList();
        boolean foundFlag = false;
        while(read < total) {
            Binary value = termReader.getBinary();
            if (value.prefixMatch(termBin, termLength)) {
                posList.add(read);
                //not exiting from the loop
//                foundFlag = true;
            } else {
                if (foundFlag) {
                    break;
                }
            }
            termReader.consume();
            read++;
        }
        long[] posArray = posList.toLongArray(new long[0]);
        return posArray;
    }

    public static long[] findTermMatchPos(ColumnReader termReader, String term) {
        Binary termBin = Binary.fromString(term);
        long total = termReader.getTotalValueCount();
        long read = 0;
        //Ideally there should be just one matching entry
        LongArrayList posList = new LongArrayList();
        boolean foundFlag = false;
        while(read < total) {
            Binary value = termReader.getBinary();
            if (value.equals(termBin)) {
                posList.add(read);
                //not exiting from the loop
//                foundFlag = true;
            } else {
                if (foundFlag) {
                    break;
                }
            }
            termReader.consume();
            read++;
        }
        long[] posArray = posList.toLongArray(new long[0]);
        return posArray;
    }

    public static SortedIntIterator getUniqueIdList(ColumnReader idReader, long[] positions) throws IOException {
        List<int[]> allIds = new ArrayList<int[]>();
        long currentPos = 0L;
        for(long pos : positions) {
            long skipCount = pos - currentPos;
            skip(idReader, skipCount);
            Binary idBin = readNextBinary(idReader);
            int[] ids = decodeInt(idBin);
            allIds.add(ids);
            currentPos = pos + 1;
        }
        if (allIds.isEmpty()) {
            return new SortedIntIterator(EMPTY);
        } else {
            return new SortedIntIterator(sort(allIds, 0, allIds.size() - 1));
        }
    }

    public static SortedIntIterator getFilteredUniqueIdList(ColumnReader idReader, long[] positions, ColumnReader filterReader) throws IOException {
        List<int[]> allIds = new ArrayList<int[]>();
        long currentPos = 0L;
        IntArrayList matchingIds = new IntArrayList();
        for(long pos : positions) {
            long skipCount = pos - currentPos;
            skip(idReader, skipCount);
            skip(filterReader, skipCount);
            Binary idBin = readNextBinary(idReader);
            Binary filterBin = readNextBinary(filterReader);
            int[] ids = decodeInt(idBin);
            BooleanIterator bools = decodeBoolean(filterBin, getCount(idBin));
            for (int i: ids) {
                boolean matchFlag = bools.nextBoolean();
                if (matchFlag == true)
                    matchingIds.add(i);
            }
            if (!matchingIds.isEmpty())
                allIds.add(matchingIds.toIntArray(new int[0]));
            matchingIds.clear();
            currentPos = pos + 1;
        }
        if (allIds.isEmpty()) {
            return new SortedIntIterator(EMPTY);
        } else {
            return new SortedIntIterator(sort(allIds, 0, allIds.size() - 1));
        }
    }

    private static void skip(ColumnReader reader, long skipCount) {
        long read = 0;
        while(read < skipCount) {
            //move to skip
            reader.getBinary();
            reader.consume();
            read++;
        }
    }

    private static Binary readNextBinary(ColumnReader reader) {
        Binary value = reader.getBinary();
        reader.consume();
        return value;
    }

    private static int[] decodeInt(Binary idBin) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(idBin.getBytes(), 0, idBin.length());
        BytesUtils.readUnsignedVarInt(in); // skip header value 1
        BytesUtils.readUnsignedVarInt(in); // skip header value 2
        int count = BytesUtils.readUnsignedVarInt(in);
        int[] ids = new int[count];
        DeltaBinaryPackingValuesReader dbpReader = new DeltaBinaryPackingValuesReader();
        dbpReader.initFromPage(count, idBin.getBytes(), 0);
        int i = 0;
        int value = 0;
        while (i < count) {
            value = dbpReader.readInteger();
            ids[i++] = value;
        }
        return ids;
    }

    private static int getCount(Binary bin) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bin.getBytes(), 0, bin.length());
        BytesUtils.readUnsignedVarInt(in); // skip header value 1
        BytesUtils.readUnsignedVarInt(in); // skip header value 2
        int count = BytesUtils.readUnsignedVarInt(in);
        return count;
    }

    private static BooleanIterator decodeBoolean(Binary boolBin, final int count) throws IOException {
        final BooleanPlainValuesReader bpvReader = new BooleanPlainValuesReader();
        bpvReader.initFromPage(count, boolBin.getBytes(), 0);
        return new BooleanIterator() {
            int read = 0;
            @Override
            public boolean nextBoolean() {
                read++;
                return bpvReader.readBoolean();
            }

            @Override
            public int skip(int i) {
                while (i >0) {
                    bpvReader.skip();
                    i--;
                }
                read = read + i;
                return i;
            }

            @Override
            public boolean hasNext() {
                return read < count;
            }

            @Override
            public Boolean next() {
                return nextBoolean();
            }
        };
    }

    // TODO: a proper implementation. This impl would assume reading it all up into memory.
    // Implement it using a heap containing buffered int iterators
    public static int[] sort(List<int[]> lists, int start, int end) {
        if (start == end) {
            return lists.get(start);
        } else {
            int mid = (start + end) / 2;
            return merge(sort(lists, start, mid), sort(lists, (mid + 1), end));
        }
    }

    private static int[] merge(int[] a, int[] b) {
        int[] c = new int[a.length + b.length];
        int ia=0;
        int ib=0;
        int ic=0;
        while (ia < a.length && ib < b.length) {
            if (a[ia] <= b[ib]) {
                c[ic++] = a[ia++];
            } else {
                c[ic++] = b[ib++];
            }
        }
        while (ia < a.length) {
            c[ic++] = a[ia++];
        }
        while (ib < b.length) {
            c[ic++] = b[ib++];
        }
        return c;
    }


}
