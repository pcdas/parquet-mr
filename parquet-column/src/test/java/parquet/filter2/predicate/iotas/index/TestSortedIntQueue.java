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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by abennett on 16/7/15.
 */
public class TestSortedIntQueue {

    @Test
    public void testEmpty() {
        int[] values = {};
        SortedIntIterator iter = new SortedIntIterator(values);
        SortedIntQueue queue = new SortedIntQueue(iter);
        queue.init();
        assertFalse(queue.checkAndPop(5));
        assertFalse(queue.checkAndPop(6));
    }

    @Test
    public void testQueue() {
        int[] values = {4,6,7,9,10,11,12,13};
        SortedIntIterator iter = new SortedIntIterator(values);
        SortedIntQueue queue = new SortedIntQueue(iter);
        queue.init();
        assertFalse(queue.checkAndPop(2));
        assertFalse(queue.checkAndPop(3));
        assertTrue(queue.checkAndPop(4));
        assertTrue(queue.checkAndPop(4));
        assertFalse(queue.checkAndPop(5));
        assertFalse(queue.checkAndPop(5));
        assertTrue(queue.checkAndPop(6));
        assertTrue(queue.checkAndPop(7));
        assertFalse(queue.checkAndPop(8));
        assertTrue(queue.checkAndPop(13));
        assertFalse(queue.checkAndPop(14));
    }
}
