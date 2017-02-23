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

package io.crate.analyze.symbol;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ParameterSymbol extends Symbol {

    private final int index;
    private final DataType type;

    public ParameterSymbol(int index, DataType type) {
        this.index = index;
        this.type = type;
    }

    public ParameterSymbol(StreamInput in) throws IOException {
        index = in.readVInt();
        type = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(index);
        DataTypes.toStream(type, out);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.PARAMETER;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitParameterSymbol(this, context);
    }

    @Override
    public DataType valueType() {
        return type;
    }

    @Override
    public String toString() {
        return "$" + Integer.toString(index + 1);
    }

    public int index() {
        return index;
    }
}
