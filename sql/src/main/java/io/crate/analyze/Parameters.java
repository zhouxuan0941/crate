/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.Row;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataType;

import static io.crate.analyze.ParameterContext.guessTypeSafe;

public final class Parameters {

    public static Literal convert(Row parameters, ParameterExpression expression) {
        int index = expression.index();
        Object val;
        try {
            val = parameters.get(index);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                "Tried to resolve a parameter but the arguments provided with the " +
                "SQLRequest don't contain a parameter at position " + index, e);
        }
        DataType type = guessTypeSafe(val);
        return Literal.of(type, type.value(val));
    }
}
