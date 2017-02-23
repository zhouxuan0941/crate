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

package io.crate.action.sql;

import io.crate.data.Row;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class RowReceiverToResultReceiverTest extends CrateUnitTest {

    @Test
    public void testSetNextRow() throws Exception {
        ResultReceiver downstream = mock(ResultReceiver.class);
        RowReceiverToResultReceiver receiver = new RowReceiverToResultReceiver(downstream, 2);
        assertThat(receiver.setNextRow(Row.EMPTY), is(RowReceiver.Result.CONTINUE));
        verify(downstream, times(1)).setNextRow(Row.EMPTY);
        assertThat(receiver.setNextRow(Row.EMPTY), is(RowReceiver.Result.PAUSE));
    }

    @Test
    public void testSetNextRowInterrupted() throws Exception {
        ResultReceiver downstream = mock(ResultReceiver.class);
        RowReceiverToResultReceiver receiver = new RowReceiverToResultReceiver(downstream, 2);
        receiver.pauseProcessed(mock(ResumeHandle.class));
        receiver.interruptIfResumable();
        assertThat(receiver.setNextRow(Row.EMPTY), is(RowReceiver.Result.STOP));
    }

    @Test
    public void testInterruptIfResumable() throws Exception {
        ResultReceiver downstream = mock(ResultReceiver.class);
        RowReceiverToResultReceiver receiver = new RowReceiverToResultReceiver(downstream, 2);
        receiver.interruptIfResumable();
        assertNull(receiver.resumeHandle());

        ResumeHandle resumeHandle = mock(ResumeHandle.class);
        receiver.pauseProcessed(resumeHandle);
        receiver.interruptIfResumable();
        verify(resumeHandle, times(1)).resume(false);
    }
}
