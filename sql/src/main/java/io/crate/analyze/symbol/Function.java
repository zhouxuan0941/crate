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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Function extends Symbol implements Cloneable {

    private final List<Symbol> arguments;
    private final FunctionInfo info;

    public Function(StreamInput in) throws IOException {
        info = new FunctionInfo();
        info.readFrom(in);
        arguments = Symbols.listFromStream(in);
    }

    public Function(FunctionInfo info, List<Symbol> arguments) {
        Preconditions.checkNotNull(info, "function info is null");
        Preconditions.checkArgument(arguments.size() == info.ident().argumentTypes().size(),
            "number of arguments must match the number of argumentTypes of the FunctionIdent");
        this.info = info;

        assert arguments.isEmpty() || !(arguments instanceof ImmutableList) :
            "must not be an immutable list - would break setArgument";
        this.arguments = arguments;
    }

    public List<Symbol> arguments() {
        return arguments;
    }

    public void setArgument(int index, Symbol symbol) {
        arguments.set(index, symbol);
    }

    public FunctionInfo info() {
        return info;
    }

    @Override
    public DataType valueType() {
        return info.returnType();
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FUNCTION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFunction(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
        Symbols.toStream(arguments, out);
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "%s(%s)", info.ident().name(), Joiner.on(",").join(arguments()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Function function = (Function) o;

        if (!arguments.equals(function.arguments)) return false;
        return info.equals(function.info);
    }

    @Override
    public int hashCode() {
        int result = arguments.hashCode();
        result = 31 * result + info.hashCode();
        return result;
    }

    @Override
    public Function clone() {
        return new Function(this.info, new ArrayList<>(this.arguments));
    }
}
