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

package io.crate.data.join;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.ImmutableList;
import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CrossJoinBlockNLBatchIteratorTest {

    private static final List<Object[]> threeXThreeRows;

    static {
        ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                builder.add(new Object[] { i, j });
            }
        }
        threeXThreeRows = builder.build();
    }

    public CrossJoinBlockNLBatchIteratorTest(String name,
                                             Supplier<BatchIterator> left,
                                             Supplier<BatchIterator> right,
                                             List<Object[]> expectedResults) {

    }

    @ParametersFactory
    public static Iterable<Object[]> testParameters() {
        return Arrays.asList(
            $(null, null, null, null),
            $(null, null, null, null)
            // TODO mxm
        );
    }

    @Test
    public void testNestedLoopBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> JoinBatchIterators.crossJoin(
                TestingBatchIterators.range(0, 3),
                TestingBatchIterators.range(0, 3),
                new CombinedRow(1, 1),
                () -> 2,
                new NoopRowAccounting()
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(threeXThreeRows);
    }

    @Test
    public void testNestedLoopWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> JoinBatchIterators.crossJoin(
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 3), 2, 2, null),
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 3), 2, 2, null),
                new CombinedRow(1, 1),
                () -> 2,
                new NoopRowAccounting()
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(threeXThreeRows);
    }

    @Test
    public void testNestedLoopLeftAndRightEmpty() throws Exception {
        BatchIterator<Row> iterator = JoinBatchIterators.crossJoin(
            InMemoryBatchIterator.empty(SENTINEL),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(0, 0),
            () -> 2,
            new NoopRowAccounting()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopLeftEmpty() throws Exception {
        BatchIterator<Row> iterator = JoinBatchIterators.crossJoin(
            InMemoryBatchIterator.empty(SENTINEL),
            TestingBatchIterators.range(0, 5),
            new CombinedRow(0, 1),
            () -> 2,
            new NoopRowAccounting()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopRightEmpty() throws Exception {
        BatchIterator<Row> iterator = JoinBatchIterators.crossJoin(
            TestingBatchIterators.range(0, 5),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(1, 0),
            () -> 2,
            new NoopRowAccounting()
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testMoveToStartWhileRightSideIsActive() {
        BatchIterator<Row> batchIterator = JoinBatchIterators.crossJoin(
            TestingBatchIterators.range(0, 3),
            TestingBatchIterators.range(10, 20),
            new CombinedRow(1, 1),
            () -> 2,
            new NoopRowAccounting()
        );

        assertThat(batchIterator.moveNext(), is(true));
        assertThat(batchIterator.currentElement().get(0), is(0));
        assertThat(batchIterator.currentElement().get(1), is(10));

        batchIterator.moveToStart();

        assertThat(batchIterator.moveNext(), is(true));
        assertThat(batchIterator.currentElement().get(0), is(0));
        assertThat(batchIterator.currentElement().get(1), is(10));
    }

    private static class NoopRowAccounting implements RowAccounting {

        @Override
        public void accountForAndMaybeBreak(Row row) {
        }

        @Override
        public void release() {
        }
    }
}
