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

package io.crate.planner;

import io.crate.analyze.TableDefinitions;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DropTablePlannerTest extends CrateUnitTest {

    private ClusterService clusterService = new NoopClusterService();
    private SQLExecutor e = SQLExecutor.builder(clusterService)
        .enableDefaultTables()
        .addBlobTable(TableDefinitions.createBlobTable(new TableIdent("blob", "screenshots"), clusterService))
        .build();

    @Test
    public void testDropTable() throws Exception {
        DropTablePlan plan = e.plan("drop table users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        Plan plan = e.plan("drop table if exists unknown_schema.unknwon_table");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        DropTablePlan plan = e.plan("drop table if exists users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsNonExistentTableCreatesNoop() throws Exception {
        Plan plan = e.plan("drop table if exists groups");
        assertThat(plan, instanceOf(NoopPlan.class));
    }


    @Test
    public void testDropPartitionedTable() throws Exception {
        DropTablePlan plan = e.plan("drop table parted");
        assertThat(plan.tableInfo().ident().name(), is("parted"));
    }

    @Test
    public void testDropBlobTableIfExistsCreatesIterablePlan() throws Exception {
        Plan plan = e.plan("drop blob table if exists screenshots");
        assertThat(plan, instanceOf(GenericDDLPlan.class));
    }

    @Test
    public void testDropNonExistentBlobTableCreatesNoop() throws Exception {
        Plan plan = e.plan("drop blob table if exists unknown");
        assertThat(plan, instanceOf(NoopPlan.class));
    }
}
