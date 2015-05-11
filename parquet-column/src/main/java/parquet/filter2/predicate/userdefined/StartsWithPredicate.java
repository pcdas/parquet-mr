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
package parquet.filter2.predicate.userdefined;

import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.io.api.Binary;

import java.io.Serializable;

/**
 * Created by abennett on 6/5/15.
 */
public class StartsWithPredicate extends UserDefinedPredicate<Binary> implements Serializable{

    private Binary target;
    private int targetLength;

    public StartsWithPredicate(String value) {
        this.target = Binary.fromString(value);
        this.targetLength = target.length();
    }

    @Override
    public boolean keep(Binary value) {
        if (value.length() < targetLength)
            return false;
        return value.prefixMatch(target, targetLength);
    }

    @Override
    public boolean canDrop(Statistics statistics) {
        return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics statistics) {
        return false;
    }

}
