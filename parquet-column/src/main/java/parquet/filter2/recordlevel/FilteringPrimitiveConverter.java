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
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.PrimitiveType;

import static parquet.Preconditions.checkNotNull;

/**
 * see {@link FilteringRecordMaterializer}
 *
 * This pass-through proxy for a delegate {@link PrimitiveConverter} also
 * updates the {@link ValueInspector}s of a {@link IncrementallyUpdatedFilterPredicate}
 */
public class FilteringPrimitiveConverter extends PrimitiveConverter {
  private final PrimitiveConverter delegate;
  private final ValueInspector[] valueInspectors;
  private boolean[] nullFlags;
  private int[] dictLookupVals;
  private boolean[] boolVals;
  private int[] intVals;
  private long[] longVals;
  private float[] floatVals;
  private double[] doubleVals;
  private Binary[] binaryVals;

  private static abstract class EndOfBatchBinding {
    abstract void endOfBatch(int filledBatchSize);
  }
  private EndOfBatchBinding batchBinding;
//  private Binding binding;

  public FilteringPrimitiveConverter(PrimitiveConverter delegate, ValueInspector[] valueInspectors) {
    this.delegate = checkNotNull(delegate, "delegate");
    this.valueInspectors = checkNotNull(valueInspectors, "valueInspectors");
  }

  @Override
  public boolean hasBatchSupport() {
    return delegate.hasBatchSupport();
  }

  @Override
  public boolean[] getNullIndicatorBatchStore(int maxBatchSize) {
    nullFlags = delegate.getNullIndicatorBatchStore(maxBatchSize);
    return nullFlags;
  }

  @Override
  public void startOfBatchOp() {
    delegate.startOfBatchOp();
  };

  @Override
  public void endOfBatchOp(int filledBatchSize) {
    batchBinding.endOfBatch(filledBatchSize);
  };

  @Override
  public boolean hasDictionarySupport() {
    // yes, only if delegate supports dictionary
    return delegate.hasDictionarySupport();
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.setDictionary(dictionary);
    }

    if (delegate.hasDictionarySupport()) {
      delegate.setDictionary(dictionary);
    }

    //bind is not needed anymore, we have verified delegate supports dictionary
//    binding = createBinding(dictionary);
  }

  @Override
  public int[] getDictLookupBatchStore(int maxBatchSize) {
    dictLookupVals = delegate.getDictLookupBatchStore(maxBatchSize);
    return dictLookupVals;
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.updateFromDictionary(dictionaryId);
    }
    //bypass bind, go directly to delegate
//    binding.writeValue(dictionaryId);
    delegate.addValueFromDictionary(dictionaryId);
  }

  @Override
  public void endOfDictBatchOp(int filledBatchSize) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.resetBatch();
      for (int i = 0; i < filledBatchSize; i++) {
        if (!nullFlags[i])
          valueInspector.updateFromDictionary(dictLookupVals[i]);
        else
          valueInspector.updateNull();
        valueInspector.prepareToSetNextResult();
      }
    }
    delegate.endOfDictBatchOp(filledBatchSize);
  }

  @Override
  public void addBinary(Binary value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBinary(value);
  }

  @Override
  public Binary[] getBinaryBatchStore(int maxBatchSize) {
    binaryVals = delegate.getBinaryBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(binaryVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return binaryVals;
  }

  @Override
  public void addBoolean(boolean value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBoolean(value);
  }

  @Override
  public boolean[] getBooleanBatchStore(int maxBatchSize) {
    boolVals = delegate.getBooleanBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(boolVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return boolVals;
  }

  @Override
  public void addDouble(double value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addDouble(value);
  }

  @Override
  public double[] getDoubleBatchStore(int maxBatchSize) {
    doubleVals = delegate.getDoubleBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(doubleVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return doubleVals;
  }

  @Override
  public void addFloat(float value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addFloat(value);
  }

  @Override
  public float[] getFloatBatchStore(int maxBatchSize) {
    floatVals = delegate.getFloatBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(floatVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return floatVals;
  }

  @Override
  public void addInt(int value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addInt(value);
  }

  @Override
  public int[] getIntBatchStore(int maxBatchSize) {
    intVals = delegate.getIntBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(intVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return intVals;
  }

  @Override
  public void addLong(long value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addLong(value);
  }


  @Override
  public long[] getLongBatchStore(int maxBatchSize) {
    longVals = delegate.getLongBatchStore(maxBatchSize);
    batchBinding = new EndOfBatchBinding() {
      void endOfBatch(int filledBatchSize) {
        for (ValueInspector valueInspector : valueInspectors) {
          valueInspector.resetBatch();
          for (int i = 0; i < filledBatchSize; i++) {
            if (!nullFlags[i])
              valueInspector.update(longVals[i]);
            else
              valueInspector.updateNull();
            valueInspector.prepareToSetNextResult();
          }
        }
        delegate.endOfBatchOp(filledBatchSize);
      }
    };
    return longVals;
  }

  /** Passes dictionary value to delegate, either using dictionary methods or add methods
   * depending upon whether the delegate supports dictionaries
   */
  private interface Binding {
    void writeValue(int dictionaryId);
  }

  /**
   * Create binding based on dictionary or on type
   * @param dictionary Dictionary used in binding
   * @return Binding that sets value on delegate
   */
  private Binding createBinding(final Dictionary dictionary) {

    if (delegate.hasDictionarySupport()) {
      return new Binding() {
        public void writeValue(int dictionaryId) {
          delegate.addValueFromDictionary(dictionaryId);
        }
      };
    } else return dictionary.getPrimitiveTypeName().convert(new PrimitiveType.PrimitiveTypeNameConverter<Binding, RuntimeException>() {
      @Override
      public Binding convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addFloat(dictionary.decodeToFloat(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addDouble(dictionary.decodeToDouble(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addInt(dictionary.decodeToInt(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addLong(dictionary.decodeToLong(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addBoolean(dictionary.decodeToBoolean(dictionaryId));
          }
        };
      }

      @Override
      public Binding convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          public void writeValue(int dictionaryId) {
            delegate.addBinary(dictionary.decodeToBinary(dictionaryId));
          }
        };
      }
    });
  }
}
