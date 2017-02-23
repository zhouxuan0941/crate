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
import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class MergeCountProjection extends Projection {

    public static final MergeCountProjection INSTANCE = new MergeCountProjection();

    private final static List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
        new Value(DataTypes.LONG)  // number of rows updated
    );

    private MergeCountProjection() {
    }

    @Override
    public List<? extends Symbol> outputs() {
        return OUTPUTS;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.CLUSTER;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.MERGE_COUNT_AGGREGATION;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitMergeCountProjection(this, context);
    }
}
