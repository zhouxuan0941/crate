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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class UndefinedType extends DataType<Object> implements DataTypeFactory, Streamer<Object> {

    public static final int ID = 0;

    public static final UndefinedType INSTANCE = new UndefinedType();

    private UndefinedType() {
    }

    @Override
    public int id() {
        return 0;
    }

    @Override
    public String getName() {
        return "null";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Object value(Object value) {
        return value;
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return true;
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        return 0;
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public Object readValueFrom(StreamInput in) throws IOException {
        return in.readGenericValue();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeGenericValue(v);
    }
}
