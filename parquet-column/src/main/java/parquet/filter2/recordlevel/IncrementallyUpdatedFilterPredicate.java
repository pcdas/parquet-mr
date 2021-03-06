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
package parquet.filter2.recordlevel;

import parquet.column.Dictionary;
import parquet.io.api.Binary;
import parquet.io.api.GroupConverter;

import java.util.BitSet;

import static parquet.Preconditions.checkNotNull;

/**
 * A rewritten version of a {@link parquet.filter2.predicate.FilterPredicate} which receives
 * the values for a record's columns one by one and internally tracks whether the predicate is
 * satisfied, unsatisfied, or unknown.
 *
 * This is used to apply a predicate during record assembly, without assembling a second copy of
 * a record, and without building a stack of update events.
 *
 * IncrementallyUpdatedFilterPredicate is implemented via the visitor pattern, as is
 * {@link parquet.filter2.predicate.FilterPredicate}
 */
public interface IncrementallyUpdatedFilterPredicate {

  /**
   * A Visitor for an {@link IncrementallyUpdatedFilterPredicate}, per the visitor pattern.
   */
  public static interface Visitor {
    boolean visit(ValueInspector p);
    boolean visit(And and);
    boolean visit(Or or);
  }

  /**
   * A {@link IncrementallyUpdatedFilterPredicate} must accept a {@link Visitor}, per the visitor pattern.
   */
  boolean accept(Visitor visitor);

  /**
   * This is the leaf node of a filter predicate. It receives the value for the primitive column it represents,
   * and decides whether or not the predicate represented by this node is satisfied.
   *
   * It is stateful, and needs to be rest after use.
   */
  public static abstract class ValueInspector implements IncrementallyUpdatedFilterPredicate {
    // package private constructor
    ValueInspector() { }

    private BitSet dictionaryResults;

    /** Populates array of BitSet based on whether values pass filter */
    void setDictionary(Dictionary dictionary) {
      dictionaryResults = new BitSet(dictionary.getMaxId()+1);
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dictionaryResults.set(i, evaluateFilterForDictionaryElement(dictionary, i));
      }
    }

    /** Update based on element from dictionary */
    void updateFromDictionary(int dictionaryId) {
      setResult(dictionaryResults.get(dictionaryId));
    }

    /** Does the element in the dictionary pass the filter */
    abstract protected boolean evaluateFilterForDictionaryElement(Dictionary dictionary, int dictionaryId);

    private boolean[] results = new boolean[GroupConverter.ROW_BATCH_SIZE];
    private boolean[] isKnowns = new boolean[GroupConverter.ROW_BATCH_SIZE];

    private int batchReadIndex = 0;
    private int batchWriteIndex = 0;

      // these methods signal what the value is
    public void updateNull() { throw new UnsupportedOperationException(); }
    public void update(int value) { throw new UnsupportedOperationException(); }
    public void update(long value) { throw new UnsupportedOperationException(); }
    public void update(double value) { throw new UnsupportedOperationException(); }
    public void update(float value) { throw new UnsupportedOperationException(); }
    public void update(boolean value) { throw new UnsupportedOperationException(); }
    public void update(Binary value) { throw new UnsupportedOperationException(); }

    /**
     * Reset to clear state and begin evaluating the next record.
     */
    public final void reset() {
      isKnowns[batchReadIndex] = false;
      ++batchReadIndex;
      if (batchReadIndex >= batchWriteIndex) {
        batchReadIndex = 0;
        batchWriteIndex = 0;
      }
    }

    public final boolean isInitialState() {
      return (batchReadIndex == 0) && (batchWriteIndex == 0) && !isKnowns[0];
    }

    public final void resetBatch() {
      while (!isInitialState()) reset();
    }

    /**
     * Subclasses should call this method to signal that the result of this predicate is known.
     */
    protected final void setResult(boolean result) {
      if (batchWriteIndex >= GroupConverter.ROW_BATCH_SIZE) {
        throw new IndexOutOfBoundsException("setResult() called on a ValueInspector exceeding the rowbatch size");
      }

      if (isKnowns[batchWriteIndex]) {
        throw new IllegalStateException("setResult() called on a ValueInspector whose result is already known!"
          + " Did you forget to call reset()?");
      }

      this.results[batchWriteIndex] = result;
      this.isKnowns[batchWriteIndex] = true;
    }

    public final void prepareToSetNextResult() {
      ++batchWriteIndex;
    }

    /**
     * Should only be called if {@link #isKnown} return true.
     */
    public final boolean getResult() {
      if (!isKnowns[batchReadIndex] || batchReadIndex > batchWriteIndex) {
        throw new IllegalStateException("getResult() called on a ValueInspector whose result is not yet known!");
      }
      // expect a call to reset to move the batch read index
      return results[batchReadIndex];
    }

    /**
     * Return true if this inspector has received a value yet, false otherwise.
     */
    public final boolean isKnown() {
      return isKnowns[batchReadIndex];
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  // base class for and / or
  static abstract class BinaryLogical implements IncrementallyUpdatedFilterPredicate {
    private final IncrementallyUpdatedFilterPredicate left;
    private final IncrementallyUpdatedFilterPredicate right;

    BinaryLogical(IncrementallyUpdatedFilterPredicate left, IncrementallyUpdatedFilterPredicate right) {
      this.left = checkNotNull(left, "left");
      this.right = checkNotNull(right, "right");
    }

    public final IncrementallyUpdatedFilterPredicate getLeft() {
      return left;
    }

    public final IncrementallyUpdatedFilterPredicate getRight() {
      return right;
    }
  }

  public static final class Or extends BinaryLogical {
    Or(IncrementallyUpdatedFilterPredicate left, IncrementallyUpdatedFilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }

  public static final class And extends BinaryLogical {
    And(IncrementallyUpdatedFilterPredicate left, IncrementallyUpdatedFilterPredicate right) {
      super(left, right);
    }

    @Override
    public boolean accept(Visitor visitor) {
      return visitor.visit(this);
    }
  }
}
