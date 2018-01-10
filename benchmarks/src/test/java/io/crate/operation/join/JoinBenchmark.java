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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

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

    @Benchmark
    public void measureSortedMerge_leftSmall(Blackhole blackhole) throws Exception {
        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.merge(
            TestingBatchIterators.range(2500, 7500),
            TestingBatchIterators.range(0, 10000),
            new CombinedRow(1, 1),
            joinCondition
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it.get(), null);
        blackhole.consume(consumer.getResult());
    }

    @Benchmark
    public void measureSortedMerge_rightSmall(Blackhole blackhole) throws Exception {
        Supplier<BatchIterator<Row>> it = () -> NestedLoopBatchIterator.merge(
            TestingBatchIterators.range(0, 10000),
            TestingBatchIterators.range(2500, 7500),
            new CombinedRow(1, 1),
            joinCondition
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it.get(), null);
        blackhole.consume(consumer.getResult());
    }
}
