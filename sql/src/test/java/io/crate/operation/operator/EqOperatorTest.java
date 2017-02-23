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

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class EqOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbol() {
        assertNormalize("2 = 2", isLiteral(true));
    }

    @Test
    public void testEqArrayLeftSideIsNull_RightSideNull() throws Exception {
        assertEvaluate("[ [1, 1], [10] ] = null", null);
        assertEvaluate("null = [ [1, 1], [10] ]", null);
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1, 1], [10] ]", isLiteral(true));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1], [10] ]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoEqualArraysShouldReturnTrueLiteral() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 1, 10]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoNotEqualArraysShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 2, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        assertNormalize("null = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        assertNormalize("2 = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        assertNormalize("2 = 4", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        assertNormalize("name = 'Arthur'", isFunction(EqOperator.NAME));
    }

    @Test
    public void testEvaluateEqOperator() {
        assertNormalize("{l=1, b=true} = {l=1, b=true}", isLiteral(true));
        assertNormalize("{l=2, b=true} = {l=1, b=true}", isLiteral(false));

        assertNormalize("1.2 = null", isLiteral(null));
        assertNormalize("'foo' = null", isLiteral(null));
    }
}
