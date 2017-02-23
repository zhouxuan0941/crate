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

import io.crate.action.sql.Option;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

@ESIntegTestCase.ClusterScope(transportClientRatio = 0)
public class OdbcIntegrationTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    private void executeQuoted(String stmt) {
        execute(stmt, null, createSession(null, EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT)));
    }

    @Before
    public void initTestData() {
        this.setup.setUpObjectTable();
        ensureYellow();
    }

    @Test
    public void testSelectDynamicQuotedObjectLiteral() throws Exception {
        executeQuoted("select \"author['name']['first_name']\", \"author['name']['last_name']\" " +
                      "from ot");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testSelectDynamicQuotedObjectLiteralWithTableAlias() throws Exception {
        executeQuoted("select \"authors\".\"author['name']['first_name']\", " +
                      "\"authors\".\"author['name']['last_name']\" " +
                      "from \"ot\" \"authors\"");
        assertEquals(1L, response.rowCount());
    }
}
