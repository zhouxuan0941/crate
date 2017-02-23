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

package io.crate.operation.operator;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class AndOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeBooleanTrueAndNonLiteral() throws Exception {
        assertNormalize("is_awesome and true", isField("is_awesome"));
    }

    @Test
    public void testNormalizeBooleanFalseAndNonLiteral() throws Exception {
        assertNormalize("is_awesome and false", isLiteral(false));
    }

    @Test
    public void testNormalizeLiteralAndLiteral() throws Exception {
        assertNormalize("true and true", isLiteral(true));
    }

    @Test
    public void testNormalizeLiteralAndLiteralFalse() throws Exception {
        assertNormalize("true and false", isLiteral(false));
    }

    @Test
    public void testEvaluateAndOperator() {
        assertEvaluate("true and true", true);
        assertEvaluate("false and false", false);
        assertEvaluate("true and false", false);
        assertEvaluate("false and true", false);
        assertEvaluate("true and null", null);
        assertEvaluate("null and true", null);
        assertEvaluate("false and null", false);
        assertEvaluate("null and false", false);
        assertEvaluate("null and null", null);
    }
}
