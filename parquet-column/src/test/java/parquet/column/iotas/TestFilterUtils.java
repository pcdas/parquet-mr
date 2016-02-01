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
package parquet.column.iotas;

import org.junit.Test;
import parquet.filter2.predicate.FilterApi;
import parquet.filter2.predicate.FilterPredicate;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by abennett on 8/7/15.
 */
public class TestFilterUtils {

    @Test
    public void testNull() {
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(null);
        assertNotNull(leafs);
        assertTrue(leafs.isEmpty());
    }

    @Test
    public void testNot() {
        FilterPredicate anyFilter = mock(FilterPredicate.class);
        FilterPredicate notFiler = FilterApi.not(anyFilter);
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(notFiler);
        assertNotNull(leafs);
        assertFalse(leafs.isEmpty());
        assertTrue(leafs.size() == 1);
        assertEquals(leafs.get(0), anyFilter);
    }

    @Test
    public void testTree() {
        FilterPredicate root = FilterApi.and(
                FilterApi.or(
                        mock(FilterPredicate.class),
                        mock(FilterPredicate.class)),
                FilterApi.not(
                        mock(FilterPredicate.class))
        );
        List<FilterPredicate> leafs = FilterUtils.getLeafNodes(root);
        assertTrue(leafs.size() == 3);
    }
}
