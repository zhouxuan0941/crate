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

package io.crate.action.sql.query;

import com.google.common.base.Throwables;
import io.crate.operation.Input;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.types.DataType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;

import java.io.IOException;
import java.util.List;

class FieldsVisitorInputFieldComparator extends InputFieldComparator {

    private final CollectorFieldsVisitor fieldsVisitor;
    private IndexReader currentReader;


    FieldsVisitorInputFieldComparator(int numHits,
                                      CollectorFieldsVisitor fieldsVisitor,
                                      Iterable<? extends LuceneCollectorExpression<?>> collectorExpressions,
                                      Input input,
                                      DataType valueType,
                                      Object missingValue) {
        super(numHits, collectorExpressions, input, valueType, missingValue);
        this.fieldsVisitor = fieldsVisitor;
        assert fieldsVisitor.required() : "Use InputFieldComparator if FieldsVisitor is not required";
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        setFieldsVisitor(doc);
        return super.compareBottom(doc);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        setFieldsVisitor(doc);
        return super.compareTop(doc);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        setFieldsVisitor(doc);
        super.copy(slot, doc);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        currentReader = context.reader();
        return super.getLeafComparator(context);
    }

    private void setFieldsVisitor(int doc) {
        fieldsVisitor.reset();
        try {
            currentReader.document(doc, fieldsVisitor);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }
}
