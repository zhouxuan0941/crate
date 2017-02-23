/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import io.crate.data.Buckets;
import io.crate.data.Row;

import java.util.List;

public class FieldExtractorRow<T> implements Row {

    private final List<Function<T, Object>> fieldExtractors;
    private T current;

    public FieldExtractorRow(List<Function<T, Object>> extractors) {
        fieldExtractors = extractors;
    }

    @Override
    public int numColumns() {
        return fieldExtractors.size();
    }

    @Override
    public Object get(int index) {
        return fieldExtractors.get(index).apply(current);
    }

    @Override
    public Object[] materialize() {
        return Buckets.materialize(this);
    }

    public void setCurrent(T current) {
        this.current = current;
    }
}
