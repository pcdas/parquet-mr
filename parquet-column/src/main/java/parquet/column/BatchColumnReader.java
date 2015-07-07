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
package parquet.column;


/**
 * Reader for (repetition level, definition level, values) triplets of a column in a batch mode.
 * This reader is used when the associated converter supports batch conversion of values.
 * 
 * @author Prakash Das
 */
public interface BatchColumnReader extends ColumnReader {
  /**
   * writes the current batch of values to the converter
   * 
   * @param batchSize
   *          the number of values in the batch, which should be less than or
   *          equal to the number of values remaining in the current page.
   * @return the number of values actually read into the batch, which is less
   *         than or equal to input value batchSize
   * @see #getRemainingPageValueCount()
   */
  int writeCurrentBatchToConverter(int batchSize);
}
