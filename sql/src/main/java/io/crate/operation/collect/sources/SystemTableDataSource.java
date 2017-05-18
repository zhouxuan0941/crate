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

package io.crate.operation.collect.sources;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.operation.reference.ReferenceResolver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class SystemTableDataSource<T> implements ReferenceResolver<RowCollectExpression<T, ?>>{

    private final Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> columnFactories;
    private final Supplier<CompletableFuture<? extends Iterable<T>>>  iterableSupplier;

    public SystemTableDataSource(Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> columnFactories,
                                 Supplier<CompletableFuture<? extends Iterable<T>>> iterableSupplier) {
        this.columnFactories = columnFactories;
        this.iterableSupplier = iterableSupplier;
    }

    @Override
    public RowCollectExpression<T, ?> getImplementation(Reference ref) {
        return rowCollectExpressionFromFactory(ref);
    }

    private RowCollectExpression<T, ?> rowCollectExpressionFromFactory(Reference ref) {
        ColumnIdent columnIdent = ref.ident().columnIdent();
        RowCollectExpressionFactory<T> columnFactory = columnFactories.get(columnIdent);
        if (columnFactory != null) {
            return columnFactory.create();
        }
        if (columnIdent.isColumn()) {
            return null;
        }
        // not a root column, traverse
        return getImplementationByRootTraversal(columnIdent);
    }

    private RowCollectExpression<T, ?> getImplementationByRootTraversal(ColumnIdent columnIdent) {
        RowCollectExpressionFactory<T> rootFactory = columnFactories.get(columnIdent.getRoot());
        if (rootFactory ==  null) {
            return null;
        }
        ReferenceImplementation<?> referenceImplementation = rootFactory.create();
        for (String part : columnIdent.path()) {
            referenceImplementation = referenceImplementation.getChildImplementation(part);
            if (referenceImplementation == null) {
                return null;
            }
        }
        return (RowCollectExpression<T, ?>)referenceImplementation;
    }
}
