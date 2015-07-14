package parquet.column.impl;

import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.io.api.Binary;

/**
 * Created by abennett on 23/7/15.
 */
public class ColumnReaderBatchingWrapper implements ColumnReader {

    private final BatchColumnReader batchColumnReader;
    private final long totalValueCount;
    private final int maxBatchSize;
    private int batchSize;
    private long readCount;

    public ColumnReaderBatchingWrapper(BatchColumnReader batchColumnReader) {

        this.batchColumnReader = batchColumnReader;
        this.totalValueCount = batchColumnReader.getTotalValueCount();
        this.maxBatchSize = batchColumnReader.getMaxBatchSize();
        this.batchSize = 0;
        this.readCount = 0;
    }


    @Override
    public long getTotalValueCount() {
        return totalValueCount;
    }

    @Override
    public void consume() {
        batchColumnReader.consume();
        readCount++;
        batchSize--;
    }

    @Override
    public long getRemainingValueCount() {
        return totalValueCount - readCount;
    }

    @Override
    public int getRemainingPageValueCount() {
        return batchColumnReader.getRemainingPageValueCount();
    }

    @Override
    public boolean isValueNull() {
        checkAndReadBatch();
        return batchColumnReader.isValueNull();
    }

    @Override
    public int getCurrentRepetitionLevel() {
        return batchColumnReader.getCurrentRepetitionLevel();
    }

    @Override
    public int getCurrentDefinitionLevel() {
        return batchColumnReader.getCurrentDefinitionLevel();
    }

    @Override
    public void writeCurrentValueToConverter() {
        throw new UnsupportedOperationException("Write to converter not supported by the batching wrapper");
    }

    @Override
    public void skip() {
        batchColumnReader.skip();
    }

    @Override
    public int getCurrentValueDictionaryID() {
        checkAndReadBatch();
        return batchColumnReader.getCurrentValueDictionaryID();
    }

    @Override
    public int getInteger() {
        checkAndReadBatch();
        return batchColumnReader.getInteger();
    }

    @Override
    public boolean getBoolean() {
        checkAndReadBatch();
        return batchColumnReader.getBoolean();
    }

    @Override
    public long getLong() {
        checkAndReadBatch();
        return batchColumnReader.getLong();
    }

    @Override
    public Binary getBinary() {
        checkAndReadBatch();
        return batchColumnReader.getBinary();
    }

    @Override
    public float getFloat() {
        checkAndReadBatch();
        return batchColumnReader.getFloat();
    }

    @Override
    public double getDouble() {
        checkAndReadBatch();
        return batchColumnReader.getDouble();
    }

    @Override
    public ColumnDescriptor getDescriptor() {
        return batchColumnReader.getDescriptor();
    }

    private void checkAndReadBatch() {
        if (batchSize == 0) {
            int c = batchColumnReader.getRemainingPageValueCount();
            batchSize = (c < maxBatchSize) ? c : maxBatchSize;
            batchColumnReader.writeCurrentBatchToConverter(batchSize);
        }
    }

    public BatchColumnReader getWrappedBatchColumnReader() {
        return batchColumnReader;
    }

}
