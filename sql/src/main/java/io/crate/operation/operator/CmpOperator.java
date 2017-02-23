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

import io.crate.core.collections.MapComparator;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;

import java.util.Map;
import java.util.Objects;

public abstract class CmpOperator extends Operator<Object> {

    /**
     * called inside {@link #normalizeSymbol(io.crate.analyze.symbol.Function)}
     * in order to interpret the result of compareTo
     * <p>
     * subclass has to implement this to evaluate the -1, 0, 1 to boolean
     * e.g. for Lt  -1 is true, 0 and 1 is false.
     *
     * @param comparisonResult the result of someLiteral.compareTo(otherLiteral)
     * @return true/false
     */
    protected abstract boolean compare(int comparisonResult);

    protected FunctionInfo info;

    protected CmpOperator(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        if (left instanceof Comparable) {
            return compare(((Comparable) left).compareTo(right));
        } else if (left instanceof Map) {
            return compare(Objects.compare((Map) left, (Map) right, MapComparator.getInstance()));
        } else {
            return null;
        }
    }

}
