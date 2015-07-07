/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.io.api;

import parquet.column.Dictionary;

/**
 * converter for leaves of the schema
 *
 * @author Julien Le Dem
 *
 */
abstract public class PrimitiveConverter extends Converter {

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public PrimitiveConverter asPrimitiveConverter() {
    return this;
  }

  /**
   * if it returns true we will attempt to use dictionary based conversion instead
   * @return if dictionary is supported
   */
  public boolean hasDictionarySupport() {
    return false;
  }

  /**
   * When this method returns true the methods related to batch conversion of
   * values such as {@link #getIntBatchStore(int) getIntBatchStore},
   * {@link #startOfBatchOp() startOfBatch},
   * {@link #endOfBatchOp(int) endOfBatch}, etc. will be invoked. These
   * methods are useful when the column values are converted as part of a row
   * with a flat schema.
   * 
   * @return true if batch is supported, false otherwise
   */
  public boolean hasBatchSupport() {
    return false;
  }

  /**
   * Set the dictionary to use if the data was encoded using dictionary encoding
   * and the converter hasDictionarySupport().
   * @param dictionary the dictionary to use for conversion
   */
  public void setDictionary(Dictionary dictionary) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** runtime calls  **/

  /**
   * add a value based on the dictionary set with setDictionary()
   * Will be used if the Converter has dictionary support and the data was encoded using a dictionary
   * @param dictionaryId the id in the dictionary of the value to add
   */
  public void addValueFromDictionary(int dictionaryId) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBinary(Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addDouble(double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addFloat(float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addInt(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addLong(long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public boolean[] getNullIndicatorBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public boolean[] getBooleanBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public int[] getIntBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public long[] getLongBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public float[] getFloatBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public double[] getDoubleBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} returns true. It will be called only once and the returned
   * array will be reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public Binary[] getBinaryBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Converter must implement this method if {@link #hasBatchSupport()
   * hasBatchSupport} and {@link #hasDictionarySupport() hasDictionarySupport}
   * returns true. It will be called only once and the returned array will be
   * reused again and again for each batch operation.
   * 
   * @param maxBatchSize
   *          maximum number of values that will be stored in a single batch
   *          operation
   * @return an array to store values during batch operation
   */
  public int[] getDictLookupBatchStore(int maxBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Indicates to a converter that a new filling of batch of values and null
   * indicators is about to start.
   */
  public void startOfBatchOp() {
    throw new UnsupportedOperationException(getClass().getName());    
  }

  /**
   * Indicates to a converter that the filling operation that has started
   * earlier ended.
   * 
   * @param filledBatchSize
   *          the number of valid values and null indicators
   */
  public void endOfBatchOp(int filledBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Indicates to a converter that the filling operation that has started
   * earlier ended by filling dictionary lookup values.
   * 
   * @param filledBatchSize
   *          the number of valid values and null indicators
   */
  public void endOfDictBatchOp(int filledBatchSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }
}
