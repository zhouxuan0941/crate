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

package io.crate.operation.reference.doc.lucene;

import io.crate.action.sql.query.SortSymbolVisitor;
import io.crate.analyze.OrderBy;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import org.apache.lucene.search.FieldDoc;

/**
 * A {@link LuceneCollectorExpression} is used to collect
 * sorting values from FieldDocs
 */
public class OrderByCollectorExpression extends LuceneCollectorExpression<Object> {

    private final int orderIndex;
    private final DataType valueType;
    private Object value;
    private Object missingValue;

    public OrderByCollectorExpression(Reference ref, OrderBy orderBy) {
        super(ref.ident().columnIdent().fqn());
        assert orderBy.orderBySymbols().contains(ref) : "symbol must be part of orderBy symbols";
        orderIndex = orderBy.orderBySymbols().indexOf(ref);
        valueType = ref.valueType();
        this.missingValue = LuceneMissingValue.missingValue(
            orderBy.reverseFlags()[orderIndex],
            orderBy.nullsFirst()[orderIndex],
            SortSymbolVisitor.LUCENE_TYPE_MAP.get(valueType)
        );
    }

    private void value(Object value) {
        if (missingValue != null && missingValue.equals(value)) {
            this.value = null;
        } else {
            this.value = valueType.value(value);
        }
    }

    public void setNextFieldDoc(FieldDoc fieldDoc) {
        value(fieldDoc.fields[orderIndex]);
    }

    @Override
    public Object value() {
        return value;
    }

}
