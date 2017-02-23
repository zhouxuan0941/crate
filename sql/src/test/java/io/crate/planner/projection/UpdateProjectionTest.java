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

package io.crate.planner.projection;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class UpdateProjectionTest {

    @Test
    public void testEquals() throws Exception {
        UpdateProjection u1 = new UpdateProjection(
            Literal.of(1), new String[]{"foo"}, new Symbol[]{Literal.of(1)}, null);

        UpdateProjection u2 = new UpdateProjection(
            Literal.of(1), new String[]{"foo"}, new Symbol[]{Literal.of(1)}, null);

        assertThat(u2.equals(u1), is(true));
        assertThat(u1.equals(u2), is(true));
        assertThat(u1.hashCode(), is(u2.hashCode()));
    }
}
