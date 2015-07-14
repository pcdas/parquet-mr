package parquet.filter2.predicate.iotas.index;

/**
 * Provides an primitive iterator (does not do autoboxing) with deduping capability
 * over a sorted int array. The input array is expected to be sorted. No defensive copy is done.
 * Created by abennett on 16/7/15.
 */
public class SortedIntIterator {

    private final int[] sortedArray;
    private final int size;
    private int i;

    public SortedIntIterator(int[] sortedArray) {
        this.sortedArray = sortedArray;
        size = sortedArray.length;
        i = -1;
    }

    public int next() {
        return sortedArray[++i];
    }

    public int nextDedupe() {
        int head = sortedArray[++i];
        while((i < size - 1) && (head == sortedArray[i + 1])) {
            ++i;
        }
        return head;
    }

    public boolean hasNext() {
        return (i < size - 1);
    }

}
