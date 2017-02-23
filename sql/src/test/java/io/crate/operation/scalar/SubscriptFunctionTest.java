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

package io.crate.operation.scalar;

import io.crate.metadata.FunctionIdent;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class SubscriptFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluate() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(1 as integer))", isLiteral("Youri"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("subscript(tags, cast(1 as integer))", isFunction("subscript"));
    }

    @Test
    public void testIndexOutOfRange() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(3 as integer))", isLiteral(null));
    }

    @Test
    public void testNotRegisteredForSets() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: subscript(integer_set, integer)");
        FunctionIdent functionIdent = new FunctionIdent(SubscriptFunction.NAME,
            Arrays.<DataType>asList(new SetType(DataTypes.INTEGER), DataTypes.INTEGER));
        functions.getSafe(functionIdent);
    }

    @Test
    public void testIndexExpressionIsNotInteger() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: subscript(string_array, long)");
        assertNormalize("subscript(['Youri', 'Ruben'], 1 + 1)", isLiteral("Ruben"));
    }
}
