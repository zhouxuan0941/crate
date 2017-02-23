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

package io.crate.analyze;

import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import java.util.Locale;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class KillAnalyzerTest extends CrateUnitTest {

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).build();

    @Test
    public void testAnalyzeKillAll() throws Exception {
        KillAnalyzedStatement stmt = e.analyze("kill all");
        assertThat(stmt.jobId().isPresent(), is(false));
    }

    @Test
    public void testAnalyzeKillJobWithParameter() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = e.analyze("kill $2", new Object[]{2, jobId});
        assertThat(stmt.jobId().get(), is(jobId));
        stmt = e.analyze("kill $1", new Object[]{jobId});
        assertThat(stmt.jobId().get(), is(jobId));
        stmt = e.analyze("kill ?", new Object[]{jobId});
        assertThat(stmt.jobId().get(), is(jobId));
    }

    @Test
    public void testAnalyzeKillJobWithLiteral() throws Exception {
        UUID jobId = UUID.randomUUID();
        KillAnalyzedStatement stmt = e.analyze(String.format(Locale.ENGLISH, "kill '%s'", jobId.toString()));
        assertThat(stmt.jobId().get(), is(jobId));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnalyzeKillJobsNotParsable() throws Exception {
        e.analyze("kill '6a3d6401-4333-933d-b38c9322fca7'");
    }
}
