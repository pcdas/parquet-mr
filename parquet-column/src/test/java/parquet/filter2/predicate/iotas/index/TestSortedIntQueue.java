package parquet.filter2.predicate.iotas.index;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by abennett on 16/7/15.
 */
public class TestSortedIntQueue {

    @Test
    public void testEmpty() {
        int[] values = {};
        SortedIntIterator iter = new SortedIntIterator(values);
        SortedIntQueue queue = new SortedIntQueue(iter);
        queue.init();
        assertFalse(queue.checkAndPop(5));
        assertFalse(queue.checkAndPop(6));
    }

    @Test
    public void testQueue() {
        int[] values = {4,6,7,9,10,11,12,13};
        SortedIntIterator iter = new SortedIntIterator(values);
        SortedIntQueue queue = new SortedIntQueue(iter);
        queue.init();
        assertFalse(queue.checkAndPop(2));
        assertFalse(queue.checkAndPop(3));
        assertTrue(queue.checkAndPop(4));
        assertTrue(queue.checkAndPop(4));
        assertFalse(queue.checkAndPop(5));
        assertFalse(queue.checkAndPop(5));
        assertTrue(queue.checkAndPop(6));
        assertTrue(queue.checkAndPop(7));
        assertFalse(queue.checkAndPop(8));
        assertTrue(queue.checkAndPop(13));
        assertFalse(queue.checkAndPop(14));
    }
}
