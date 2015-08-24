package parquet.column.iotas;

import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;

import java.util.HashMap;
import java.util.Map;

import static parquet.Preconditions.checkNotNull;

/**
 * Wrapper over the default PageReadStore.
 * Supports multiple page read stores, namespaced by the schema name
 *
 * Created by abennett on 23/6/15.
 */
public class EmbeddedTablePageReadStore implements PageReadStore{

    private final PageReadStore mainPageReadStore;
    private final Map<String, PageReadStore> embeddedPageReadStores;

    public EmbeddedTablePageReadStore(PageReadStore mainPageReadStore) {
        this.mainPageReadStore = mainPageReadStore;
        this.embeddedPageReadStores = new HashMap<String, PageReadStore>();
    }

    public void addPageReadStore(String schemaName, PageReadStore pageReadStore) {
        checkNotNull(schemaName, "schemaName");
        checkNotNull(pageReadStore, "pageReadStore");
        this.embeddedPageReadStores.put(schemaName, pageReadStore);
    }

    public PageReadStore getPageReadStore(String schemaName) {
        checkNotNull(schemaName, "schemaName");
        return this.embeddedPageReadStores.get(schemaName);
    }

    @Override
    public PageReader getPageReader(ColumnDescriptor descriptor) {
        return mainPageReadStore.getPageReader(descriptor);
    }

    public PageReader getPageReader(String schemaName, ColumnDescriptor descriptor) {
        checkNotNull(schemaName, "schemaName");
        return this.embeddedPageReadStores.get(schemaName).getPageReader(descriptor);
    }

    @Override
    public long getRowCount() {
        return mainPageReadStore.getRowCount();
    }
}
