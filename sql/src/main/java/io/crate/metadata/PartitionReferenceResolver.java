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

package io.crate.metadata;

import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.partitioned.PartitionExpression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionReferenceResolver implements ReferenceResolver<PartitionExpression> {

    private final Map<ReferenceIdent, PartitionExpression> expressionMap;
    private final List<PartitionExpression> partitionExpressions;

    public PartitionReferenceResolver(List<PartitionExpression> partitionExpressions) {
        this.partitionExpressions = partitionExpressions;
        this.expressionMap = new HashMap<>(partitionExpressions.size(), 1.0f);
        for (PartitionExpression partitionExpression : partitionExpressions) {
            expressionMap.put(partitionExpression.reference().ident(), partitionExpression);
        }
    }

    @Override
    public PartitionExpression getImplementation(Reference ref) {
        PartitionExpression expression = expressionMap.get(ref.ident());
        assert expression != null : "granularity < PARTITION should have been resolved already";
        return expression;
    }

    public List<PartitionExpression> expressions() {
        return partitionExpressions;
    }
}
