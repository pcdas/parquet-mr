package parquet.column.iotas;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.impl.BatchColumnReaderImpl;
import parquet.column.impl.ColumnReaderBatchingWrapper;
import parquet.column.impl.ColumnReaderImpl;
import parquet.column.page.PageReader;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
 * Created by abennett on 24/6/15.
 */
public class EmbeddedTableColumnReaderFactory {

    private final EmbeddedTablePageReadStore pageReadStore;
    private final boolean useBatchedRead;
    private static final PrimitiveConverter NOOP_CONVERTER = new NoOpPrimitiveConverter();

    public EmbeddedTableColumnReaderFactory(EmbeddedTablePageReadStore pageReadStore,
                                            boolean useBatchedRead) {
        this.pageReadStore = pageReadStore;
        this.useBatchedRead = useBatchedRead;
    }

    /**
     * Creates a reader for the specified column.
     * @param schema Name of the embedded schema. Should match the parquet table name
     * @param column
     * @return
     */
    public ColumnReader getColumnReader(String schema, ColumnDescriptor column) {
        PageReader pageReader = this.pageReadStore.getPageReader(schema, column);
        if (useBatchedRead) {
            return new ColumnReaderBatchingWrapper(new BatchColumnReaderImpl(column, pageReader));
        } else {
            return new ColumnReaderImpl(column, pageReader, NOOP_CONVERTER);
        }
    }

    /**
     * NoOp primitive converter. This is never expected to be used for consuming values, and any
     * column reader using this converter is supposed to be used via the ColumnReader interface -
     * that is, the values should be read directly, and not via a converter.
     */
    private static class NoOpPrimitiveConverter extends PrimitiveConverter {
    }
}
