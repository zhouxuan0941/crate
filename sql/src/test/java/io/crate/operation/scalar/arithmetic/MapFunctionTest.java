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

package io.crate.operation.scalar.arithmetic;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MapFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testMapWithWrongNumOfArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: _map(string, long, string)");
        assertEvaluate("_map('foo', 1, 'bar')", null);
    }

    @Test
    public void testKeysMustBeOfTypeString() throws Exception {
        expectedException.expectMessage("unknown function: _map(long, long)");
        assertEvaluate("_map(10, 2)", null);
    }

    @Test
    public void testEvaluation() throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("foo", 10L);
        // minimum args
        assertEvaluate("_map('foo', 10)", m);

        // variable args
        m.put("bar", new BytesRef("some"));
        assertEvaluate("_map('foo', 10, 'bar', 'some')", m);
    }
}
