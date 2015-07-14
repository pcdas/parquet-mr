package parquet.column.impl;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
* Created by abennett on 23/7/15.
*/
class DefaultBatchPrimitiveConverter extends PrimitiveConverter {
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
  public void startOfBatchOp() {}

  @Override
  public void endOfBatchOp(int filledBatchSize) {}
}
