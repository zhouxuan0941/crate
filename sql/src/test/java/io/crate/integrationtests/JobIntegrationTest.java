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
import io.crate.action.sql.SQLOperations;
import io.crate.plugin.CrateCorePlugin;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.UseJdbc;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import javax.annotation.Nullable;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, randomDynamicTemplates = false)
@UseJdbc
public class JobIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("plugin.types", CrateCorePlugin.class.getName())
            .build();
    }

    public JobIntegrationTest() {
        // ensure that the client node is used as handler and has no collectphase
        super(new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return internalCluster().clientNodeClient();
                }

                @Nullable
                @Override
                public String pgUrl() {
                    return null;
                }

                @Override
                public SQLOperations sqlOperations() {
                    return internalCluster().getInstance(SQLOperations.class);
                }
            }
        ));
    }

    @Test
    public void testFailurePropagationNonLocalCollectPhase() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("\"_version\" column is not valid in the WHERE clause");
        execute("create table users (name string) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into users (name) (select name from users where _version = 1)");
        execute("select name from users where _version = 1");
    }
}
