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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
/**
 * Created by abennett on 16/7/15.
 */
public class TestSortedIntIterator {

    @Test
    public void testEmpty() {
        SortedIntIterator iter = new SortedIntIterator(new int[0]);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSingle() {
        int[] values = {0};
        SortedIntIterator iter = new SortedIntIterator(values);
        List<Integer> results = new ArrayList<Integer>();
        while(iter.hasNext())
            results.add(iter.next());
        assertTrue(results.size() == 1);
        assertTrue(results.contains(0));
    }

    @Test
    public void testDedupe() {
        int[] values = {0,0,0};
        SortedIntIterator iter = new SortedIntIterator(values);
        List<Integer> results = new ArrayList<Integer>();
        while(iter.hasNext())
            results.add(iter.nextDedupe());
        assertTrue(results.size() == 1);
        assertTrue(results.contains(0));
    }

    @Test
    public void testMany() {
        int[] values = {0,0,1,2,2,3};
        SortedIntIterator iter = new SortedIntIterator(values);
        List<Integer> results = new ArrayList<Integer>();
        while(iter.hasNext())
            results.add(iter.next());
        assertTrue(results.size() == 6);
        assertTrue(results.contains(0));
        assertTrue(results.contains(1));
        assertTrue(results.contains(2));
        assertTrue(results.contains(3));

        iter = new SortedIntIterator(values);
        results = new ArrayList<Integer>();
        while(iter.hasNext())
            results.add(iter.nextDedupe());
        assertTrue(results.size() == 4);
        assertTrue(results.contains(0));
        assertTrue(results.contains(1));
        assertTrue(results.contains(2));
        assertTrue(results.contains(3));
    }
}
