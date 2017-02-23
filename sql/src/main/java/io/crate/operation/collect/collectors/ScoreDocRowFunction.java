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

package io.crate.operation.collect.collectors;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import io.crate.data.Row;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class ScoreDocRowFunction implements Function<ScoreDoc, Row> {

    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final IndexReader indexReader;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final DummyScorer scorer;
    private final InputRow inputRow;


    public ScoreDocRowFunction(IndexReader indexReader,
                               List<Input<?>> inputs,
                               Collection<? extends LuceneCollectorExpression<?>> expressions,
                               DummyScorer scorer) {
        this.indexReader = indexReader;
        this.expressions = expressions;
        this.scorer = scorer;
        this.inputRow = new InputRow(inputs);
        addOrderByExpressions(expressions);
    }

    private void addOrderByExpressions(Collection<? extends LuceneCollectorExpression<?>> expressions) {
        for (LuceneCollectorExpression<?> expression : expressions) {
            if (expression instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression) expression);
            }
        }
    }

    @Nullable
    @Override
    public Row apply(@Nullable ScoreDoc input) {
        if (input == null) {
            return null;
        }
        FieldDoc fieldDoc = (FieldDoc) input;
        scorer.score(fieldDoc.score);
        for (OrderByCollectorExpression orderByCollectorExpression : orderByCollectorExpressions) {
            orderByCollectorExpression.setNextFieldDoc(fieldDoc);
        }
        List<LeafReaderContext> leaves = indexReader.leaves();
        int readerIndex = ReaderUtil.subIndex(fieldDoc.doc, leaves);
        LeafReaderContext subReaderContext = leaves.get(readerIndex);
        int subDoc = fieldDoc.doc - subReaderContext.docBase;
        for (LuceneCollectorExpression<?> expression : expressions) {
            try {
                expression.setNextReader(subReaderContext);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            expression.setNextDocId(subDoc);
        }
        return inputRow;
    }
}
