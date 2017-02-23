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

import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;

import java.util.function.DoubleUnaryOperator;

/**
 * Scalar implementation that wraps another function: f(double) -> double
 * <br />
 * null values will result in null as output
 *
 */
public final class DoubleScalar extends Scalar<Double, Number> {

    private final FunctionInfo info;
    private final DoubleUnaryOperator func;

    public DoubleScalar(FunctionInfo info, DoubleUnaryOperator func) {
        this.info = info;
        this.func = func;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @SafeVarargs
    @Override
    public final Double evaluate(Input<Number>... args) {
        assert args.length == 1 : "DoubleScalar expects exactly 1 argument, got: " + args.length;
        Number value = args[0].value();
        if (value == null) {
            return null;
        }
        return func.applyAsDouble(value.doubleValue());
    }
}
