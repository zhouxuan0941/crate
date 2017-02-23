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

package io.crate.executor.transport.task;

import io.crate.data.Row;
import io.crate.executor.transport.kill.KillAllRequest;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.elasticsearch.action.ActionListener;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class KillTaskTest extends CrateUnitTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testKillTaskCallsBroadcastOnTransportKillAllNodeAction() throws Exception {
        TransportKillAllNodeAction killAllNodeAction = mock(TransportKillAllNodeAction.class);
        KillTask task = new KillTask(killAllNodeAction, UUID.randomUUID());

        task.execute(new CollectingRowReceiver(), Row.EMPTY);
        verify(killAllNodeAction, times(1)).broadcast(any(KillAllRequest.class), any(ActionListener.class));
        verify(killAllNodeAction, times(0)).nodeOperation(any(KillAllRequest.class), any(ActionListener.class));
    }
}
