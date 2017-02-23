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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.UseJdbc;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

@UseJdbc
public class SysClusterTest extends SQLTransportIntegrationTest {

    @Test
    public void testSysCluster() throws Exception {
        execute("select id");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) response.rows()[0][0]).length(), is(36)); // looks like a uuid
    }

    public void testSysClusterMasterNode() throws Exception {
        execute("select id from sys.nodes");
        List<String> nodes = new ArrayList<>();
        for (Object[] nodeId : response.rows()) {
            nodes.add((String) nodeId[0]);
        }

        execute("select master_node from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        String node = (String) response.rows()[0][0];
        assertTrue(nodes.contains(node));
    }

    @Test
    public void testExplainSysCluster() throws Exception {
        execute("explain select * from sys.cluster limit 2"); // using limit to test projection serialization as well
        assertThat(response.rowCount(), is(1L));
        Map<String, Object> map = (Map<String, Object>) response.rows()[0][0];
        assertThat(map.get("planType"), is("Collect"));
    }

    @Test
    public void testScalarEvaluatesInErrorOnSysCluster() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select 1/0 from sys.cluster");
    }
}
