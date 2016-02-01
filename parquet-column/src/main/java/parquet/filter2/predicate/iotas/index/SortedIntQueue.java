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
package parquet.filter2.predicate.iotas.index;

/**
 * Checks for membership in a sorted queue. The values has to be checked in an ascending fashion.
 * Created by abennett on 16/7/15.
 */
public class SortedIntQueue {

    private final SortedIntIterator values;
    private boolean hasValue;
    private int currentValue;

    public SortedIntQueue(SortedIntIterator values) {
        this.values = values;
        this.hasValue = false;
        this.currentValue = 0;
    }

    public void init() {
        if (values.hasNext()) {
            hasValue = true;
            currentValue = values.nextDedupe();
        }

    }

    public boolean checkAndPop(int value) {
        if (!hasValue) {
            return false;
        }
        if (value < currentValue) {
            return false;
        } else if (value == currentValue) {
            return true;
        } else {
            while (hasValue && value > currentValue) {
                readNext();
            }
            return checkAndPop(value);
        }

    }

    private void readNext() {
        if (values.hasNext()) {
            currentValue = values.nextDedupe();
        } else {
            hasValue = false;
        }
    }

}
