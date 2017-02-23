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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.InputColumn;
import io.crate.metadata.RowGranularity;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

public class FilterProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES, T3.TR_1);

        FilterProjection p = new FilterProjection(
            sqlExpressions.normalize(sqlExpressions.asSymbol("a = 'foo'")),
            ImmutableList.of(new InputColumn(1))
        );
        p.requiredGranularity(RowGranularity.SHARD);

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        StreamInput in = StreamInput.wrap(out.bytes());
        FilterProjection p2 = (FilterProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }
}
