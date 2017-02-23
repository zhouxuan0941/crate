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

package io.crate.planner.projection;

import com.google.common.base.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.operation.projectors.TopN;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class TopNProjection extends Projection {

    private final int limit;
    private final int offset;
    private final List<Symbol> outputs;

    public TopNProjection(int limit, int offset, List<Symbol> outputs) {
        assert limit > TopN.NO_LIMIT : "limit of TopNProjection must not be negative/unlimited";

        this.limit = limit;
        this.offset = offset;
        this.outputs = outputs;
    }

    public TopNProjection(StreamInput in) throws IOException {
        offset = in.readVInt();
        limit = in.readVInt();
        outputs = Symbols.listFromStream(in);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(outputs, replaceFunction);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.TOPN;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitTopNProjection(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(offset);
        out.writeVInt(limit);
        Symbols.toStream(outputs, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopNProjection that = (TopNProjection) o;

        if (limit != that.limit) return false;
        if (offset != that.offset) return false;
        if (!outputs.equals(that.outputs)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = limit;
        result = 31 * result + offset;
        result = 31 * result + outputs.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopNProjection{" +
               "outputs=" + outputs +
               ", limit=" + limit +
               ", offset=" + offset +
               '}';
    }

}
