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

package io.crate.integrationtests;

import io.crate.data.Bucket;
import io.crate.planner.Merge;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
@UseJdbc
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    private void setUpCharacters() {
        sqlExecutor.exec("create table characters (id int primary key, name string) " +
                         "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureYellowOrGreen();
        sqlExecutor.execBulk("insert into characters (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{1, "Arthur"},
                new Object[]{2, "Ford"},
            }
        );
        sqlExecutor.exec("refresh table characters");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();
        PlanForNode plan = plan("select id, name, substr(name, 2) from characters order by id");
        QueryThenFetch qtf = ((QueryThenFetch) plan.plan);
        assertThat(qtf.subPlan(), instanceOf(Merge.class));
        Merge merge = (Merge) qtf.subPlan();

        assertThat(((FetchProjection) merge.mergePhase().projections().get(0)).nodeReaders(), notNullValue());
        assertThat(((FetchProjection) merge.mergePhase().projections().get(0)).readerIndices(), notNullValue());

        CollectingRowReceiver rowReceiver = execute(plan);

        Bucket result = rowReceiver.result();
        assertThat(result.size(), is(2));
        assertThat(rowReceiver.rows.get(0).length, is(3));
        assertThat(rowReceiver.rows.get(0)[0], is(1));
        assertThat(rowReceiver.rows.get(0)[1], is(new BytesRef("Arthur")));
        assertThat(rowReceiver.rows.get(0)[2], is(new BytesRef("rthur")));
        assertThat(rowReceiver.rows.get(1)[0], is(2));
        assertThat(rowReceiver.rows.get(1)[1], is(new BytesRef("Ford")));
        assertThat(rowReceiver.rows.get(1)[2], is(new BytesRef("ord")));
    }
}
