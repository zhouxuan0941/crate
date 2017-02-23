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

import io.crate.testing.SQLResponse;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SysJobsTest extends SQLTransportIntegrationTest {

    @After
    public void resetStatsEnabled() throws Exception {
        execute("reset global stats.enabled");
    }

    @Test
    public void testQueryAllColumns() throws Exception {
        execute("set global stats.enabled = true");
        String stmt = "select * from sys.jobs";

        // the response contains all current jobs, if the tests are executed in parallel
        // this might be more then only the "select * from sys.jobs" statement.
        SQLResponse response = execute(stmt);
        List<String> statements = new ArrayList<>();

        for (Object[] objects : response.rows()) {
            assertNotNull(objects[0]);
            statements.add((String) objects[2]);
        }
        assertTrue(statements.contains(stmt));
    }
}
