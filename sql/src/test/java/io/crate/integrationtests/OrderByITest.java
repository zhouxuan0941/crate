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
import org.hamcrest.core.Is;
import org.junit.Test;

public class OrderByITest extends SQLTransportIntegrationTest {

    @Test
    public void testOrderByIpType() throws Exception {
        execute("create table t1 (" +
                "  ipp ip" +
                ")");
        ensureYellow();
        execute("insert into t1 (ipp) values (?)", new Object[][]{
            {"127.0.0.1"},
            {null},
            {"10.0.0.1"},
        });
        execute("refresh table t1");
        execute("select ipp from t1 order by ipp");
        assertThat(TestingHelpers.printedTable(response.rows()), Is.is(
            "10.0.0.1\n" +
            "127.0.0.1\n" +
            "NULL\n"));

        execute("select ipp from t1 order by ipp desc nulls first");
        assertThat(TestingHelpers.printedTable(response.rows()), Is.is(
            "NULL\n" +
            "127.0.0.1\n" +
            "10.0.0.1\n"));
    }

}
