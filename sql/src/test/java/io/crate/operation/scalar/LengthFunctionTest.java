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

import io.crate.analyze.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;

public class LengthFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testOctetLengthEvaluateOnString() throws Exception {
        assertEvaluate("octet_length('©rate')", 6);
        assertEvaluate("octet_length('crate')", 5);
        assertEvaluate("octet_length('')", 0);
    }

    @Test
    public void testBitLengthEvaluateOnString() throws Exception {
        assertEvaluate("bit_length('©rate')", 48);
        assertEvaluate("bit_length('crate')", 40);
        assertEvaluate("bit_length('')", 0);
    }

    @Test
    public void testCharLengthEvaluateOnString() throws Exception {
        assertEvaluate("char_length('©rate')", 5);
        assertEvaluate("char_length('crate')", 5);
        assertEvaluate("char_length('')", 0);
    }

    @Test
    public void testOctetLengthEvaluateOnNull() throws Exception {
        assertEvaluate("octet_length(null)", null);
        assertEvaluate("octet_length(name)", null, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testBitLengthEvaluateOnNull() throws Exception {
        assertEvaluate("bit_length(null)", null);
        assertEvaluate("bit_length(name)", null, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testCharLengthEvaluateOnNull() throws Exception {
        assertEvaluate("char_length(null)", null);
        assertEvaluate("char_length(name)", null, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("bit_length(name)", isFunction("bit_length"));
        assertEvaluate("bit_length(name)", 16, Literal.of("©"));
        assertEvaluate("octet_length(name)", 2, Literal.of("©"));
        assertEvaluate("char_length(name)", 1, Literal.of("©"));
    }
}
