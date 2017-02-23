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

package io.crate.operation.scalar.conditional;

import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;

import java.util.Comparator;

abstract class ConditionalCompareFunction extends ConditionalFunction implements Comparator {

    ConditionalCompareFunction(FunctionInfo info) {
        super(info);
    }

    @Override
    public Object evaluate(Input... args) {
        assert args != null : "args must not be null";
        assert args.length > 0 : "number of args must be > 1";

        if (args.length == 1) {
            return args[0].value();
        }

        Object result = null;
        for (Input input : args) {
            result = extrema(result, input.value());
        }

        return result;
    }

    private Object extrema(Object value1, Object value2) {
        if (value1 == null) {
            return value2;
        } else if (value2 == null) {
            return value1;
        }
        return compare(value1, value2) < 0 ? value1 : value2;
    }
}
