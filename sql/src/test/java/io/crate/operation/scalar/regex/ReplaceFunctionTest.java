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

package io.crate.operation.scalar.regex;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class ReplaceFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate(
            "regexp_replace(name, '(ba)', 'Crate')",
            "fooCraterbequebaz bar",
            Literal.of("foobarbequebaz bar"));
    }

    @Test
    public void testEvaluateWithFlags() throws Exception {
        assertEvaluate(
            "regexp_replace(name, '(ba)', 'Crate', 'usn g')",
            "fooCraterbequebaz bar",
            Literal.of("foobarbequebaz bar"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("regexp_replace('foobarbequebaz bar', '(ba)', 'Crate')", isLiteral("fooCraterbequebaz bar"));
        assertNormalize("regexp_replace(name, '(ba)', 'Crate')", isFunction(ReplaceFunction.NAME));
    }

    @Test
    public void testNormalizeSymbolWithFlags() throws Exception {
        assertNormalize("regexp_replace('foobarbequebaz bar', '(ba)', 'Crate', 'us n')", isLiteral("fooCraterbequebaz bar"));
    }

    @Test
    public void testNormalizeSymbolWithInvalidFlags() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: regexp_replace(string, string, string, long)");

        assertNormalize("regexp_replace('foobar', 'foo', 'bar', 1)", isLiteral(""));
    }

    @Test
    public void testNormalizeSymbolWithInvalidNumberOfArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        assertNormalize("regexp_replace('foobar')", isLiteral(""));
    }

    @Test
    public void testNormalizeSymbolWithInvalidArgumentType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: regexp_replace(string, string, long)");

        assertNormalize("regexp_replace('foobar', '.*', 1)", isLiteral(""));
    }
}
