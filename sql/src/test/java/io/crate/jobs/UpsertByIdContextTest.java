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

package io.crate.jobs;

import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.planner.node.dml.UpsertById;
import io.crate.test.CauseMatcher;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

public class UpsertByIdContextTest extends CrateUnitTest {

    @Mock
    public BulkRequestExecutor delegate;

    private UpsertByIdContext context;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ShardUpsertRequest request = mock(ShardUpsertRequest.class);
        UpsertById.Item item = mock(UpsertById.Item.class);
        context = new UpsertByIdContext(1, request, item, delegate);
    }

    @Test
    public void testKill() throws Exception {
        ArgumentCaptor<ActionListener> listener = ArgumentCaptor.forClass(ActionListener.class);
        context.prepare();
        context.start();
        verify(delegate).execute(any(ShardUpsertRequest.class), listener.capture());

        // context is killed
        context.kill(null);
        // listener returns
        ShardResponse response = mock(ShardResponse.class);
        listener.getValue().onResponse(response);

        expectedException.expectCause(CauseMatcher.cause(InterruptedException.class));
        context.completionFuture().get();
    }

    @Test
    public void testKillBeforeStart() throws Exception {
        context.prepare();
        context.kill(null);
        expectedException.expectCause(CauseMatcher.cause(InterruptedException.class));
        context.completionFuture().get();
    }

    @Test
    public void testStartAfterClose() throws Exception {
        context.prepare();
        context.close();

        context.start();
        // start does nothing, because the context is already closed
        verify(delegate, never()).execute(any(ShardUpsertRequest.class), any(ActionListener.class));
    }
}
