package parquet.column.iotas;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.impl.BatchColumnReaderImpl;
import parquet.column.impl.ColumnReaderImpl;
import parquet.column.page.PageReader;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
 * Created by abennett on 24/6/15.
 */
public class MultiSchemaColumnReaderFactory {

    private final MultiSchemaPageReadStore pageReadStore;
    private final boolean useBatchedRead;
    private static final PrimitiveConverter NOOP_CONVERTER = new NoOpPrimitiveConverter();

    public MultiSchemaColumnReaderFactory(MultiSchemaPageReadStore pageReadStore,
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
            return new BatchColumnReaderImpl(column, pageReader, NOOP_CONVERTER);
        } else {
            return new ColumnReaderImpl(column, pageReader, NOOP_CONVERTER);
        }
    }

    /**
     * NoOp primitive converter. This is never expected to be used for consuming values, and any
     * column reader using this converter is supposed to be used via the ColumnReader interface -
     * that is, the values should be read directly, and not via a converter.
     *
     * Implements storage so that it works with with BatchedColumnReader
     */
    private static class NoOpPrimitiveConverter extends PrimitiveConverter {

        @Override
        public boolean hasBatchSupport() {
            return true;
        }

        @Override
        public boolean[] getNullIndicatorBatchStore(int maxBatchSize) {
            return new boolean[maxBatchSize];
        }

        @Override
        public boolean[] getBooleanBatchStore(int maxBatchSize) {
            return new boolean[maxBatchSize];
        }

        @Override
        public int[] getIntBatchStore(int maxBatchSize) {
            return new int[maxBatchSize];
        }

        @Override
        public long[] getLongBatchStore(int maxBatchSize) {
            return new long[maxBatchSize];
        }

        @Override
        public float[] getFloatBatchStore(int maxBatchSize) {
            return new float[maxBatchSize];
        }

        @Override
        public double[] getDoubleBatchStore(int maxBatchSize) {
            return new double[maxBatchSize];
        }

        @Override
        public Binary[] getBinaryBatchStore(int maxBatchSize) {
            return new Binary[maxBatchSize];
        }

        @Override
        public int[] getDictLookupBatchStore(int maxBatchSize) {
            return new int[maxBatchSize];
        }

        @Override
        public void startOfBatchOp() {

        }

        @Override
        public void endOfBatchOp(int filledBatchSize) {

        }

        @Override
        public void endOfDictBatchOp(int filledBatchSize) {

        }
    }
}
