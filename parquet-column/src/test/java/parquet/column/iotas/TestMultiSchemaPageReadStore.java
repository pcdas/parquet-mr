package parquet.column.iotas;

import org.junit.Test;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by abennett on 8/7/15.
 */
public class TestMultiSchemaPageReadStore {

    @Test
    public void testDelegation() {
        PageReadStore mainStore = mock(PageReadStore.class);
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        PageReader pageReader = mock(PageReader.class);
        when(mainStore.getPageReader(column)).thenReturn(pageReader);
        when(mainStore.getRowCount()).thenReturn(10L);
        MultiSchemaPageReadStore pageReadStore = new MultiSchemaPageReadStore(mainStore);
        assertEquals(pageReadStore.getPageReader(column), pageReader);
        assertEquals(pageReadStore.getRowCount(), 10L);
    }

    @Test(expected = NullPointerException.class)
    public void testAddNullSchema() {
        PageReadStore mainStore = mock(PageReadStore.class);
        MultiSchemaPageReadStore pageReadStore = new MultiSchemaPageReadStore(mainStore);
        pageReadStore.addPageReadStore(null, mock(PageReadStore.class));
    }

    @Test(expected = NullPointerException.class)
    public void testAddNullEmbeddedStore() {
        PageReadStore mainStore = mock(PageReadStore.class);
        MultiSchemaPageReadStore pageReadStore = new MultiSchemaPageReadStore(mainStore);
        pageReadStore.addPageReadStore("test", null);
    }

    @Test
    public void testEmbeddedStore() {
        MultiSchemaPageReadStore pageReadStore = new MultiSchemaPageReadStore(mock(PageReadStore.class));
        PageReadStore embeddedStore = mock(PageReadStore.class);
        ColumnDescriptor column = mock(ColumnDescriptor.class);
        PageReader pageReader = mock(PageReader.class);
        when(embeddedStore.getPageReader(column)).thenReturn(pageReader);
        pageReadStore.addPageReadStore("test", embeddedStore);
        assertEquals(pageReadStore.getPageReadStore("test"), embeddedStore);
        assertEquals(pageReadStore.getPageReader("test", column), pageReader);
        assertEquals(pageReadStore.getPageReadStore("test").getPageReader(column), pageReader);
    }

}
