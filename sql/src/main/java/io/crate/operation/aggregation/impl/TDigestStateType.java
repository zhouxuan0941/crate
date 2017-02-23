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

package io.crate.operation.aggregation.impl;

import io.crate.Streamer;
import io.crate.types.DataType;
import io.crate.types.DataTypeFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class TDigestStateType extends DataType<TDigestState> implements Streamer<TDigestState>, DataTypeFactory {

    static final int ID = 5120;
    static final TDigestStateType INSTANCE = new TDigestStateType();

    private TDigestStateType() {
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public TDigestState readValueFrom(StreamInput in) throws IOException {
        return TDigestState.read(in);
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        TDigestState.write((TDigestState) v, out);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "percentile_state";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public TDigestState value(Object value) throws IllegalArgumentException, ClassCastException {
        return (TDigestState) value;
    }

    @Override
    public int compareValueTo(TDigestState val1, TDigestState val2) {
        return 0;
    }
}
