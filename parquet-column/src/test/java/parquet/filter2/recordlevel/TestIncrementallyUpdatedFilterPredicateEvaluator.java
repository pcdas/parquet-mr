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

import org.junit.Test;

import parquet.column.Dictionary;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.Or;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateEvaluator.evaluate;

public class TestIncrementallyUpdatedFilterPredicateEvaluator {

  public static class ShortCircuitException extends RuntimeException {
    public ShortCircuitException() {
      super("this was supposed to short circuit and never get here!");
    }
  }

  public static ValueInspector intIsNull() {
    return new ValueInspector() {
      @Override
      protected boolean evaluateFilterForDictionaryElement(Dictionary dictionary, int dictionaryId) {
        return false;
      }

      @Override
      public void updateNull() {
        setResult(true);
      }

      @Override
      public void update(int value) {
        setResult(false);
      }
    };
  }

  public static ValueInspector intIsEven() {
    return new ValueInspector() {

      @Override
      protected boolean evaluateFilterForDictionaryElement(Dictionary dictionary, int dictionaryId) {
        return dictionary.decodeToInt(dictionaryId) % 2 == 0;
      }

      @Override
      public void updateNull() {
        setResult(false);
      }

      @Override
      public void update(int value) {
        setResult(value % 2 == 0);
      }
    };
  }

  public static ValueInspector doubleMoreThan10() {
    return new ValueInspector() {

      @Override
      protected boolean evaluateFilterForDictionaryElement(Dictionary dictionary, int dictionaryId) {
        return dictionary.decodeToDouble(dictionaryId) > 10.0;
      }

      @Override
      public void updateNull() {
        setResult(false);
      }

      @Override
      public void update(double value) {
        setResult(value > 10.0);
      }
    };
  }

  @Test
  public void testValueInspector() {
    // known, and set to false criteria, null considered false
    ValueInspector v = intIsEven();
    v.update(11);
    assertFalse(evaluate(v));
    v.reset();

    // known and set to true criteria, null considered false
    v.update(12);
    assertTrue(evaluate(v));
    v.reset();

    // known and set to null, null considered false
    v.updateNull();
    assertFalse(evaluate(v));
    v.reset();

    // known, and set to false criteria, null considered true
    ValueInspector intIsNull = intIsNull();
    intIsNull.update(10);
    assertFalse(evaluate(intIsNull));
    intIsNull.reset();

    // known, and set to false criteria, null considered true
    intIsNull.updateNull();
    assertTrue(evaluate(intIsNull));
    intIsNull.reset();

    // unknown, null considered false
    v.reset();
    assertFalse(evaluate(v));

    // unknown, null considered true
    intIsNull.reset();
    assertTrue(evaluate(intIsNull));
  }

  @Test
  public void testBatchModeValueInspector() {
    // known, and set to false criteria, null considered false
    ValueInspector v = intIsEven();
    v.resetBatch(); // start of batch operation
    for (int i = 11; i < 20; ++i) {
      v.update(i);
      v.prepareToSetNextResult();
    }
    // known and set to null, null considered false
    v.updateNull();
    v.prepareToSetNextResult();
   
    // retrieve results that were set earlier
    for (int i = 11; i < 20; ++i) {
      if (i % 2 == 0)
        assertTrue(evaluate(v));
      else
        assertFalse(evaluate(v));
      v.reset();
    }
    assertFalse(evaluate(v));
    v.reset();

    // unknown, null considered false
    // v.resetBatch(); is not necessary all the results have been retrieved.
    assertFalse(evaluate(v));

    // known, and set to false criteria, null considered true
    ValueInspector intIsNull = intIsNull();
    intIsNull.resetBatch(); // start of batch operation
    intIsNull.update(10); // 1st result
    intIsNull.prepareToSetNextResult();
    // known, and set to false criteria, null considered true
    intIsNull.updateNull(); // 2nd result
    intIsNull.prepareToSetNextResult();
    intIsNull.updateNull(); // 3rd result
    intIsNull.prepareToSetNextResult();
   
    // retrieve results 1st & 2nd
    assertFalse(evaluate(intIsNull));
    intIsNull.reset();
    assertTrue(evaluate(intIsNull));
    intIsNull.reset();
    
    // unknown, null considered true
    // resetBatch() is necessary because the 3rd result set above wasn't retrieved
    intIsNull.resetBatch();
    assertTrue(evaluate(intIsNull));
  }

  private void doOrTest(ValueInspector v1, ValueInspector v2, int v1Value, int v2Value, boolean expected) {
    v1.update(v1Value);
    v2.update(v2Value);
    IncrementallyUpdatedFilterPredicate or = new Or(v1, v2);
    assertEquals(expected, evaluate(or));
    v1.reset();
    v2.reset();
  }

  private void doAndTest(ValueInspector v1, ValueInspector v2, int v1Value, int v2Value, boolean expected) {
    v1.update(v1Value);
    v2.update(v2Value);
    IncrementallyUpdatedFilterPredicate and = new And(v1, v2);
    assertEquals(expected, evaluate(and));
    v1.reset();
    v2.reset();
  }


  @Test
  public void testOr() {
    ValueInspector v1 = intIsEven();
    ValueInspector v2 = intIsEven();

    int F = 11;
    int T = 12;

    // F || F == F
    doOrTest(v1, v2, F, F, false);
    // F || T == T
    doOrTest(v1, v2, F, T, true);
    // T || F == T
    doOrTest(v1, v2, T, F, true);
    // T || T == T
    doOrTest(v1, v2, T, T, true);

  }

  @Test
  public void testAnd() {
    ValueInspector v1 = intIsEven();
    ValueInspector v2 = intIsEven();

    int F = 11;
    int T = 12;

    // F && F == F
    doAndTest(v1, v2, F, F, false);
    // F && T == F
    doAndTest(v1, v2, F, T, false);
    // T && F == F
    doAndTest(v1, v2, T, F, false);
    // T && T == T
    doAndTest(v1, v2, T, T, true);

  }

  @Test
  public void testShortCircuit() {
    ValueInspector neverCalled = new ValueInspector() {

      @Override
      protected boolean evaluateFilterForDictionaryElement(Dictionary dictionary, int dictionaryId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean accept(Visitor visitor) {
        throw new ShortCircuitException();
      }
    };

    try {
      evaluate(neverCalled);
      fail("this should throw");
    } catch (ShortCircuitException e) {
      //
    }

    // T || X should evaluate to true without inspecting X
    ValueInspector v = intIsEven();
    v.update(10);
    IncrementallyUpdatedFilterPredicate or = new Or(v, neverCalled);
    assertTrue(evaluate(or));
    v.reset();

    // F && X should evaluate to false without inspecting X
    v.update(11);
    IncrementallyUpdatedFilterPredicate and = new And(v, neverCalled);
    assertFalse(evaluate(and));
    v.reset();

  }

}
