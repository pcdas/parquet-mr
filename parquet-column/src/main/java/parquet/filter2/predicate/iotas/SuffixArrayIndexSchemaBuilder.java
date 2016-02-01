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

import static parquet.filter2.predicate.iotas.SuffixArrayPredicate.TABLE_NAME;
import static parquet.filter2.predicate.iotas.SuffixArrayPredicate.TERM_COLUMN;
import static parquet.filter2.predicate.iotas.SuffixArrayPredicate.DOC_IDS_COLUMN;
import static parquet.filter2.predicate.iotas.SuffixArrayPredicate.STARTS_WITH_FLAG_COLUMN;
/**
 * Created by abennett on 24/9/15.
 */
public class SuffixArrayIndexSchemaBuilder {

    private String tableName;
    private boolean termColumnFlag;
    private boolean docIdsColumnFlag;
    private boolean startsWithColumnFlag;

    private SuffixArrayIndexSchemaBuilder() {
        this.tableName = TABLE_NAME;
        this.termColumnFlag = false;
        this.docIdsColumnFlag = false;
        this.startsWithColumnFlag = false;
    }

    public static SuffixArrayIndexSchemaBuilder newBuilder() {
        return new SuffixArrayIndexSchemaBuilder();
    }

    public SuffixArrayIndexSchemaBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SuffixArrayIndexSchemaBuilder withTermColumn() {
        this.termColumnFlag = true;
        return this;
    }

    public SuffixArrayIndexSchemaBuilder withDocIdsColumn() {
        this.docIdsColumnFlag= true;
        return this;
    }

    public SuffixArrayIndexSchemaBuilder withStartsWithColumn() {
        this.startsWithColumnFlag = true;
        return this;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ");
        sb.append(tableName);
        sb.append(" {");
        if (termColumnFlag) {
            sb.append(" required binary ");
            sb.append(TERM_COLUMN);
            sb.append(" (UTF8);");
        }
        if (docIdsColumnFlag) {
            sb.append(" required binary ");
            sb.append(DOC_IDS_COLUMN);
            sb.append(";");
        }
        if (startsWithColumnFlag) {
            sb.append(" required binary ");
            sb.append(STARTS_WITH_FLAG_COLUMN);
            sb.append(";");
        }
        sb.append(" }");
        return sb.toString();
    }
}
