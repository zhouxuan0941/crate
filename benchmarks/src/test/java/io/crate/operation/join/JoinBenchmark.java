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

package io.crate.operation.join;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.NestedLoopBatchIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import javafx.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class JoinBenchmark {

    private static final Predicate<Row> joinCondition = row -> Objects.equals(row.get(0), row.get(1));
    //
//    @Benchmark
//    public void measurePlainNL_leftSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.plainNL(
//            TestingBatchIterators.range(2500, 7500),
//            TestingBatchIterators.range(0, 10000),
//            new CombinedRow(1, 1),
//            joinCondition
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }
//
//    @Benchmark
//    public void measurePlainNL_rightSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.plainNL(
//            TestingBatchIterators.range(0, 10000),
//            TestingBatchIterators.range(2500, 7500),
//            new CombinedRow(1, 1),
//            joinCondition
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }
//
//    @Benchmark
//    public void measureBlockNL_leftSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.blockNL(
//            TestingBatchIterators.range(2500, 7500),
//            TestingBatchIterators.range(0, 10000),
//            new CombinedRow(1, 1),
//            joinCondition,
//            5000
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }
//
//    @Benchmark
//    public void measureBlockNL_rightSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.blockNL(
//            TestingBatchIterators.range(0, 10000),
//            TestingBatchIterators.range(2500, 7500),
//            new CombinedRow(1, 1),
//            joinCondition,
//            10000
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }
//
//    @Benchmark
//    public void measureBlockHash_leftSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.hashNL(
//            TestingBatchIterators.range(2500, 7500),
//            TestingBatchIterators.range(0, 10000),
//            new CombinedRow(1, 1),
//            joinCondition,
//            5000
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }
//
//    @Benchmark
//    public void measureBlockHash_rightSmall(Blackhole blackhole) throws Exception {
//        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.hashNL(
//            TestingBatchIterators.range(0, 10000),
//            TestingBatchIterators.range(2500, 7500),
//            new CombinedRow(1, 1),
//            joinCondition,
//            10000
//        );
//        TestingRowConsumer consumer = new TestingRowConsumer();
//        consumer.accept(it.get(), null);
//        blackhole.consume(consumer.getResult());
//    }

    private static class RowComparator implements Comparator<Row> {

        @Override
        public int compare(Row r1, Row r2) {
            return (Integer) r1.get(0) - (Integer) r2.get(0);
        }
    }

    private static final RowComparator ROW_COMPARATOR = new RowComparator();

    @State(Scope.Thread)
    public static class IteratorAndResultState {

        BatchIterator<Row> leftSmallIterator;
        BatchIterator<Row> shuffleLeftSmallIterator;
        BatchIterator<Row> shuffleRightSmallIterator;
        BatchIterator<Row> rightSmallIterator;
        TestingRowConsumer consumer;
        List<Object[]> result;
        List<? extends Row> leftSmallLeftItems;
        List<? extends Row> leftSmallRightItems;
        List<? extends Row> rightSmallLeftItems;
        List<? extends Row> rightSmallRightItems;

        @Setup(Level.Invocation)
        public void setup() {
            leftSmallIterator = NestedLoopBatchIterator.merge(
                TestingBatchIterators.range(2500, 7500),
                TestingBatchIterators.range(0, 10000),
                new CombinedRow(1, 1),
                joinCondition);

            Pair<BatchIterator<Row>, List<? extends Row>> leftSmallShuffleLeftPair = TestingBatchIterators.shuffleRange(2500, 7500);
            leftSmallLeftItems = leftSmallShuffleLeftPair.getValue();
            Pair<BatchIterator<Row>, List<? extends Row>> leftSmallShffleRightPair = TestingBatchIterators.shuffleRange(0, 10000);
            leftSmallRightItems = leftSmallShffleRightPair.getValue();

            shuffleLeftSmallIterator = NestedLoopBatchIterator.merge(
                leftSmallShuffleLeftPair.getKey(),
                leftSmallShffleRightPair.getKey(),
                new CombinedRow(1, 1),
                joinCondition);

            rightSmallIterator = NestedLoopBatchIterator.merge(
                TestingBatchIterators.range(0, 10000),
                TestingBatchIterators.range(2500, 7500),
                new CombinedRow(1, 1),
                joinCondition
            );

            Pair<BatchIterator<Row>, List<? extends Row>> rightSmallShuffleLeftPair = TestingBatchIterators.shuffleRange(0, 10000);
            rightSmallLeftItems = rightSmallShuffleLeftPair.getValue();
            Pair<BatchIterator<Row>, List<? extends Row>> rightSmallShuffleRightPair = TestingBatchIterators.shuffleRange(2500, 7500);
            rightSmallRightItems = rightSmallShuffleRightPair.getValue();
            shuffleRightSmallIterator = NestedLoopBatchIterator.merge(
                rightSmallShuffleLeftPair.getKey(),
                rightSmallShuffleRightPair.getKey(),
                new CombinedRow(1, 1),
                joinCondition
            );
            consumer = new TestingRowConsumer();
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            leftSmallIterator = null;
            leftSmallLeftItems = null;
            leftSmallRightItems = null;

            rightSmallIterator = null;
            rightSmallLeftItems = null;
            rightSmallRightItems = null;

            consumer = null;
            result = null;
        }
    }

    @Benchmark
    public void measureSortedMerge_leftSmall(IteratorAndResultState state) throws Exception {
        state.consumer.accept(state.leftSmallIterator, null);
        state.result = state.consumer.getResult();
    }

    @Benchmark
    public void measureSortedMerge_rightSmall(IteratorAndResultState state) throws Exception {
        state.consumer.accept(state.rightSmallIterator, null);
        state.result = state.consumer.getResult();
    }

    @Benchmark
    public void measureSortedMerge_shuffleLeftSmall(IteratorAndResultState state) throws Exception {
        Collections.sort(state.leftSmallLeftItems, ROW_COMPARATOR);
        Collections.sort(state.leftSmallRightItems, ROW_COMPARATOR);
        // need to get a java.util.Iterator from the new shuffled lists inside the InMemoryBatchIterator
        state.shuffleLeftSmallIterator.moveToStart();
        state.consumer.accept(state.shuffleLeftSmallIterator, null);
        state.result = state.consumer.getResult();
    }

    @Benchmark
    public void measureSortedMerge_shuffleRightSmall(IteratorAndResultState state) throws Exception {
        Collections.sort(state.rightSmallLeftItems, ROW_COMPARATOR);
        Collections.sort(state.rightSmallRightItems, ROW_COMPARATOR);
        // need to get a java.util.Iterator from the new shuffled lists inside the InMemoryBatchIterator
        state.shuffleRightSmallIterator.moveToStart();
        state.consumer.accept(state.shuffleRightSmallIterator, null);
        state.result = state.consumer.getResult();
    }
}
