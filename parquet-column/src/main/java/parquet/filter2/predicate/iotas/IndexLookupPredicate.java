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

import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.userdefined.EmbeddedTableDataConsumer;
import parquet.schema.MessageType;

/**
 * Interface to be implemented by all UDPs that does index lookup.The UDP should be able to provide
 * the metadata around the index it is going to read. This metadata is used to identify the embedded
 * table (that contains the index) to be used for the evaluation of the predicate.
 *
 * For example, an instance of the IndexLookupPredicate could need the 'suffix-array' type index
 * created against the 'doc_value' column of the main table. Further, it would need the 'term' and
 * 'pos' columns to be read from the index.
 *
 * Created by abennett on 26/6/15.
 */
public interface IndexLookupPredicate extends EmbeddedTableDataConsumer {

    /**
     * @return The type of index that the predicate needs for its functioning. The only index type
     * supported currently is 'suffix-array'.
     */
    public String getIndexTypeName();

    /**
     * @return The projection list to be used while reading the index table.
     */
    public MessageType getRequestedSchema();

    /**
     * @return The column of base table on which the index is created.
     */
    public ColumnPath getIndexedColumn();
}
