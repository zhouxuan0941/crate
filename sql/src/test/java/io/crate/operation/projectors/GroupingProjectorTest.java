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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.AggregationContext;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.collect.CollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;


public class GroupingProjectorTest extends CrateUnitTest {

    protected static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    /**
     * NOTE:
     * <p>
     * the remaining tests for the GroupingProjector are in {@link io.crate.operation.projectors.ProjectionToProjectorVisitorTest}
     **/

    @Test
    public void testAggregationToPartial() throws Exception {

        ImmutableList<Input<?>> keys = ImmutableList.<Input<?>>of(
            new DummyInput(new BytesRef("one"), new BytesRef("one"), new BytesRef("three")));


        FunctionInfo countInfo = new FunctionInfo(new FunctionIdent("count", ImmutableList.<DataType>of()), DataTypes.LONG);
        Aggregation countAggregation =
            Aggregation.partialAggregation(countInfo, DataTypes.LONG, ImmutableList.<Symbol>of());

        Functions functions = getFunctions();

        AggregationContext aggregationContext = new AggregationContext(
            (AggregationFunction) functions.get(countInfo.ident()),
            countAggregation);

        AggregationContext[] aggregations = new AggregationContext[]{aggregationContext};
        GroupingProjector projector = new GroupingProjector(
            Arrays.asList(DataTypes.STRING),
            keys,
            new CollectExpression[0],
            aggregations,
            RAM_ACCOUNTING_CONTEXT
        );

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);

        Row emptyRow = new RowN(new Object[]{});

        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.setNextRow(emptyRow);
        projector.finish(RepeatHandle.UNSUPPORTED);
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(2));
        assertThat(rows.iterator().next().get(1), instanceOf(CountAggregation.LongState.class));
    }

    class DummyInput implements Input<BytesRef> {

        private final BytesRef[] values;
        private int idx;

        DummyInput(BytesRef... values) {
            this.values = values;
            this.idx = 0;
        }

        @Override
        public BytesRef value() {
            return values[idx++];
        }
    }
}
