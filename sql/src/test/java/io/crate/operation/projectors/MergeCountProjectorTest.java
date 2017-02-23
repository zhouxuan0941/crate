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

package io.crate.operation.projectors;

import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class MergeCountProjectorTest extends CrateUnitTest {

    @Test
    public void testMergeCountProject() throws Throwable {
        Projector projector = new MergeCountProjector();
        Row row = new Row1(1L);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        projector.downstream(rowReceiver);

        for (int i = 0; i < 10; i++) {
            projector.setNextRow(row);
        }
        projector.finish(RepeatHandle.UNSUPPORTED);
        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(1));
        assertThat(projected.iterator().next().get(0), instanceOf(Long.class));
        assertThat((Long) projected.iterator().next().get(0), is(10L));
    }
}
