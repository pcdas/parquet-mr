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
package parquet.filter2.predicate.iotas;

import parquet.column.ColumnReader;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.filter2.predicate.iotas.index.SortedIntIterator;
import parquet.filter2.predicate.iotas.index.SortedIntQueue;
import parquet.filter2.predicate.iotas.index.SuffixArrayUtils;

import java.io.IOException;

import static parquet.Preconditions.checkNotNull;

/**
 * Created by abennett on 14/7/15.
 */
public class WordStartsWithPredicate extends SuffixArrayPredicate {

    private static final String INDEX_SCHEMA = SuffixArrayIndexSchemaBuilder.newBuilder()
            .withTermColumn()
            .withDocIdsColumn()
            .withStartsWithColumn().build();

    private final String columnName;
    private final String term;

    public WordStartsWithPredicate(String indexTableName, String columnName, String term) {
        super(INDEX_SCHEMA, indexTableName, columnName);
        checkNotNull(indexTableName, "indexTableName");
        this.columnName = checkNotNull(columnName, "columnName");
        this.term = checkNotNull(term, "term").toLowerCase();
    }

    @Override
    public void init(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        try {
            ColumnReader termColumn = getTermColumn(columnReaderFactory);
            ColumnReader docIdColumn = getDocIdColumn(columnReaderFactory);
            ColumnReader flagColumn = getWordStartFlagColumn(columnReaderFactory);
            long[] pos = SuffixArrayUtils.findTermStartsWithPos(termColumn, term);
            SortedIntIterator docIdIterator =
                    SuffixArrayUtils.getFilteredUniqueIdList(docIdColumn, pos, flagColumn);
            SortedIntQueue docIds = new SortedIntQueue(docIdIterator);
            docIds.init();
            setFilteredDocIds(docIds);
        } catch (IOException e) {
            throwCorruptIndexException(e);
        }
    }
}
