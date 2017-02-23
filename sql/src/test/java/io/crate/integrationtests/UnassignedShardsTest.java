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

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
@UseJdbc
public class UnassignedShardsTest extends SQLTransportIntegrationTest {

    @Test
    public void testOnlyUnassignedShards() throws Exception {
        execute("set global transient cluster.routing.allocation.enable=none");
        try {
            execute("create table no_shards (id int) clustered into 5 shards with (number_of_replicas=2)");
            execute("select state, id, table_name from sys.shards where schema_name='doc' AND table_name='no_shards'");
            assertThat(response.rowCount(), is(15L));
            Object[] stateColumn = TestingHelpers.getColumn(response.rows(), 0);
            for (Object val : stateColumn) {
                assertThat((String) val, is("UNASSIGNED"));
            }
        } finally {
            execute("reset global cluster.routing.allocation.enable");
        }
    }
}
