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

package io.crate.analysis.tree;

import javax.annotation.Nullable;
import java.util.List;

public final class AnalysedQuerySpec extends AnalysedRelation {

    private final boolean distinct;
    private final List<TypedExpression> selectList;
    private final List<AnalysedRelation> sources;
    private final TypedExpression where;
    private final List<TypedExpression> groupBy;
    private final TypedExpression having;
    private final List<TypedExpression> orderBy;
    private final TypedExpression limit;
    private final TypedExpression offset;

    public AnalysedQuerySpec(boolean distinct,
                             List<TypedExpression> selectList,
                             List<AnalysedRelation> sources,
                             @Nullable TypedExpression where,
                             List<TypedExpression> groupBy,
                             @Nullable TypedExpression having,
                             List<TypedExpression> orderBy,
                             @Nullable TypedExpression limit,
                             TypedExpression offset) {
        this.distinct = distinct;
        this.selectList = selectList;
        this.sources = sources;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "AnalysedQuerySpec{" +
               "distinct=" + distinct +
               ", selectList=" + selectList +
               ", sources=" + sources +
               ", where=" + where +
               ", groupBy=" + groupBy +
               ", having=" + having +
               ", orderBy=" + orderBy +
               ", limit=" + limit +
               ", offset=" + offset +
               '}';
    }

    @Nullable
    @Override
    public TypedExpression get(String name) {
        throw new UnsupportedOperationException("NYI");
    }
}
