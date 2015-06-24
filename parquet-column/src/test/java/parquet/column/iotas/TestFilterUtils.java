package parquet.column.iotas;

import org.junit.Test;
import parquet.filter2.predicate.FilterApi;
import parquet.filter2.predicate.FilterPredicate;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by abennett on 8/7/15.
 */
public class TestFilterUtils {

    @Test
    public void testNull() {
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(null);
        assertNotNull(leafs);
        assertTrue(leafs.isEmpty());
    }

    @Test
    public void testNot() {
        FilterPredicate anyFilter = mock(FilterPredicate.class);
        FilterPredicate notFiler = FilterApi.not(anyFilter);
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(notFiler);
        assertNotNull(leafs);
        assertFalse(leafs.isEmpty());
        assertTrue(leafs.size() == 1);
        assertEquals(leafs.get(0), anyFilter);
    }

    @Test
    public void testTree() {
        FilterPredicate root = FilterApi.and(
                FilterApi.or(
                        mock(FilterPredicate.class),
                        mock(FilterPredicate.class)),
                FilterApi.not(
                        mock(FilterPredicate.class))
        );
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(root);
        assertTrue(leafs.size() == 3);
    }
}
