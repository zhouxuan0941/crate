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

import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.AggregateCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.IntegerColumnReference;
import io.crate.types.DataTypes;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.test.TestSearchContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.mock;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AggregationFrameworkBenchmark {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT = new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
    private IndexSearcher indexSearcher;
    private AggregateCollector collector;
    private SumAggregator sumAggregator;
    private LuceneBatchIterator batchIterator;

    @Setup
    public void setupCollectorsAndAggregations() throws Exception {
        // insert documents
        IndexWriter iw = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(new StandardAnalyzer()));
        String columnName = "x";
        for (int i = 0; i < 10_000_000; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(columnName, i));
            iw.addDocument(doc);
        }
        iw.commit();
        iw.forceMerge(1, true);
        indexSearcher = new IndexSearcher(DirectoryReader.open(iw));

        // needed by the crate benchmark
        IntegerColumnReference columnReference = new IntegerColumnReference(columnName);
        List<IntegerColumnReference> columnRefs = Collections.singletonList(columnReference);

        CollectorContext collectorContext = new CollectorContext(
            mock(IndexFieldDataService.class),
            new CollectorFieldsVisitor(0)
        );
        InputCollectExpression inExpr0 = new InputCollectExpression(0);
        SumAggregation sumAggregation = ((SumAggregation) getFunctions().getBuiltin(
            SumAggregation.NAME, Collections.singletonList(DataTypes.INTEGER)));
        collector = new AggregateCollector(
            Collections.singletonList(inExpr0),
            RAM_ACCOUNTING_CONTEXT,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { sumAggregation },
            new Input[] { inExpr0 }
        );
        batchIterator = new LuceneBatchIterator(
            indexSearcher,
            new MatchAllDocsQuery(),
            null,
            false,
            collectorContext,
            RAM_ACCOUNTING_CONTEXT,
            columnRefs,
            columnRefs
        );

        // needed by the ES benchmark
        TestSearchContext context = new TestSearchContext(null);
        IndexNumericFieldData indexFieldData = new SortedNumericDVIndexFieldData(
            new Index("foo", UUID.randomUUID().toString()),
            "x",
            IndexNumericFieldData.NumericType.INT);
        ValuesSource.Numeric valuesSource = new ValuesSource.Numeric.FieldData(indexFieldData);
        sumAggregator = new SumAggregator("x_sum", valuesSource, DocValueFormat.RAW, context,
            null, null, null);
    }

    @Benchmark
    public Object[] measureCrateAggregation() {
        Object[] state = collector.supplier().get();
        BiConsumer<Object[], Row> accumulator = collector.accumulator();
        Function<Object[], Object[]> finisher = collector.finisher();

        while (batchIterator.moveNext()) {
            accumulator.accept(state, batchIterator.currentElement());
        }
        return finisher.apply(state);
    }

    @Benchmark
    public Double measureESAggregation() throws Exception {
        sumAggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), sumAggregator);
        return sumAggregator.metric(0);
    }
}
