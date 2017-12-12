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

package io.crate.operation.collect.collectors;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.InputRow;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.types.DataTypes;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LuceneStream {

    public static Stream<Row> stream(IndexSearcher searcher,
                                     Query query,
                                     @Nullable Float minScore,
                                     boolean doScores,
                                     CollectorContext collectorContext,
                                     RamAccountingContext ramAccountingContext,
                                     List<? extends Input<?>> inputs,
                                     Collection<? extends LuceneCollectorExpression<?>> expressions) throws IOException {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        CollectorFieldsVisitor visitor = collectorContext.visitor();
        Weight weight = searcher.createNormalizedWeight(query, doScores || minScore != null);
        InputRow inputRow = new InputRow(inputs);
        return searcher.getTopReaderContext().leaves().stream()
            .flatMap(readerContext -> {
                Scorer scorer;
                try {
                    scorer = weight.scorer(readerContext);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (scorer == null) {
                    return Stream.empty();
                }
                for (LuceneCollectorExpression<?> expression : expressions) {
                    try {
                        expression.setNextReader(readerContext);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    expression.setScorer(scorer);
                }
                return StreamSupport.stream(
                    new RowSpliterator(readerContext, scorer, visitor, expressions, inputRow, minScore),
                    false
                );
            });
    }

    private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
        if (liveDocs == null) {
            return false;
        }
        return liveDocs.get(doc) == false;
    }

    private static class RowSpliterator implements Spliterator<Row> {

        private final Scorer scorer;
        private final CollectorFieldsVisitor visitor;
        private final Collection<? extends LuceneCollectorExpression<?>> expressions;
        private final InputRow inputRow;
        private final Float minScore;
        private final LeafReader reader;
        private final Bits liveDocs;
        private final DocIdSetIterator docIt;

        public RowSpliterator(LeafReaderContext readerContext,
                              Scorer scorer,
                              CollectorFieldsVisitor visitor,
                              Collection<? extends LuceneCollectorExpression<?>> expressions,
                              InputRow inputRow,
                              Float minScore) {
            this.reader = readerContext.reader();
            this.scorer = scorer;
            this.visitor = visitor;
            this.expressions = expressions;
            this.inputRow = inputRow;
            this.minScore = minScore;
            this.liveDocs = reader.getLiveDocs();
            this.docIt = scorer.iterator();
        }

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            int doc;
            try {
                while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (docDeleted(liveDocs, doc) || (minScore != null && scorer.score() < minScore)) {
                        continue;
                    }
                    if (visitor.required()) {
                        visitor.reset();
                        reader.document(doc, visitor);
                    }
                    for (LuceneCollectorExpression<?> expression : expressions) {
                        expression.setNextDocId(doc);
                    }
                    StringJoiner joiner = new StringJoiner(", ");
                    for (int i = 0; i < inputRow.numColumns(); i++) {
                        BytesRef value = DataTypes.STRING.value(inputRow.get(i));
                        if (value == null) {
                            joiner.add("NULL");
                        } else {
                            joiner.add(value.utf8ToString());
                        }
                    }
                    System.out.println("## doc=" + doc + ", " + joiner.toString());
                    action.accept(inputRow);
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return false;
        }

        @Override
        public Spliterator<Row> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }
}
