/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import io.crate.Streamer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RowType extends DataType<Map<String, Object>> {

    public static final RowType EMPTY = new RowType(Collections.emptyList(), Collections.emptyList());
    private final List<String> names;
    private final List<DataType> types;

    public RowType(List<String> names, List<DataType> types) {
        this.names = names;
        this.types = types;
    }

    public List<String> names() {
        return names;
    }

    public List<DataType> types() {
        return types;
    }

    @Override
    public int id() {
        return 0;
    }

    @Override
    public String getName() {
        return "row";
    }

    @Override
    public Streamer<?> streamer() {
        return null;
    }

    @Override
    public Map<String, Object> value(Object value) throws IllegalArgumentException, ClassCastException {
        return null;
    }

    @Override
    public int compareValueTo(Map<String, Object> val1, Map<String, Object> val2) {
        return 0;
    }

    @Override
    public String toString() {
        return "RowType{" +
               "names=" + names +
               ", types=" + types +
               '}';
    }
}
