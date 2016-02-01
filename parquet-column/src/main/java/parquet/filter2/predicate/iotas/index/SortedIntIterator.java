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
 * Provides an primitive iterator (does not do autoboxing) with deduping capability
 * over a sorted int array. The input array is expected to be sorted. No defensive copy is done.
 * Created by abennett on 16/7/15.
 */
public class SortedIntIterator {

    private final int[] sortedArray;
    private final int size;
    private int i;

    public SortedIntIterator(int[] sortedArray) {
        this.sortedArray = sortedArray;
        size = sortedArray.length;
        i = -1;
    }

    public int next() {
        return sortedArray[++i];
    }

    public int nextDedupe() {
        int head = sortedArray[++i];
        while((i < size - 1) && (head == sortedArray[i + 1])) {
            ++i;
        }
        return head;
    }

    public boolean hasNext() {
        return (i < size - 1);
    }

}
