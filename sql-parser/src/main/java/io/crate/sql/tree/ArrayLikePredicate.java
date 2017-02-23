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

package io.crate.sql.tree;

/**
 * ATTENTION! pattern and value swapped. use left and right
 * because they appear on wrong order in ANY LIKE predicate.
 * <p>
 * <code>&lt;PATTERN&gt; LIKE (&lt;COLUMN&gt;) [ ESCAPE &lt;ESCAPE_VALUE&gt; ]</code>
 * </p>
 * <p>
 * left is pattern <br>
 * right is the array expression
 */
public class ArrayLikePredicate extends LikePredicate implements ArrayComparison {

    private final Quantifier quantifier;
    private final boolean inverse;

    /**
     * @param quantifier      quantifier of comparison operation
     * @param arrayExpression array/set expression to apply the like operation on
     * @param pattern         the like pattern used
     * @param escape          the escape value
     * @param inverse         if true, inverse the operation for every single comparison
     */
    public ArrayLikePredicate(Quantifier quantifier,
                              Expression arrayExpression,
                              Expression pattern,
                              Expression escape,
                              boolean inverse) {
        super(pattern, arrayExpression, escape);
        this.quantifier = quantifier;
        this.inverse = inverse;
    }

    public Quantifier quantifier() {
        return quantifier;
    }

    public boolean inverse() {
        return inverse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ArrayLikePredicate that = (ArrayLikePredicate) o;

        if (inverse != that.inverse) return false;
        if (quantifier != that.quantifier) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + quantifier.hashCode();
        result = 31 * result + (inverse ? 1 : 0);
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrayLikePredicate(this, context);
    }
}
