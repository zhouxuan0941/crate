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

package io.crate.analyze.where;

import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.any.AnyEqOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <pre>
 *     {@code
 *
 *                or
 *               /  \______
 *              /          \
 *             and          and
 *             /  \         /  \
 *            /    \       /    \
 *       id1=?   id2=?   id1=?   id2=?
 *
 *
 *            and
 *            /   \_____
 *           /          \
 *       id1 in (...)   id2 in (...)
 *     }
 * </pre>
 */
public final class EqualsExtractor {

    public static List<Match> extractMatches(List<ColumnIdent> columnsToFind, Symbol symbolTree) {
        ComparisonBuilder builder = new ComparisonBuilder(columnsToFind);
        Visitor.INSTANCE.process(symbolTree, builder);
        return builder.generateMatches();
    }

    public static class Match {
        List<EqualPair> pairs = new ArrayList<>();

        @Override
        public String toString() {
            return pairs
                .stream()
                .map(p -> p.function)
                .map(SymbolPrinter.INSTANCE::printSimple)
                .collect(Collectors.joining(", "));
        }
    }

    private static class EqualPair {
        ColumnIdent column;
        Function function;

        EqualPair(ColumnIdent column, Function fun) {
            this.column = column;
            this.function = fun;
        }

        Symbol valueSymbol() {
            return function.arguments().get(1);
        }
    }

    private static class ComparisonBuilder {

        private final Collection<ColumnIdent> columnsToFind;
        private final ArrayList<Match> comparisons = new ArrayList<>();
        private Match currentOrBranch = new Match();

        boolean seenOther = false;

        ComparisonBuilder(Collection<ColumnIdent> columnsToFind) {
            this.columnsToFind = columnsToFind;
            this.comparisons.add(currentOrBranch);
        }

        void registerEquals(Function fun) {
            Symbol left = fun.arguments().get(0);
            if (SymbolVisitors.any(Symbols.IS_COLUMN, fun.arguments().get(1))) {
                seenOther = true;
                return;
            }

            if (left instanceof Reference) {
                if (columnsToFind.contains(((Reference) left).column())) {
                    currentOrBranch.pairs.add(new EqualPair(((Reference) left).column(), fun));
                    return;
                }
            } else if (left instanceof Field) {
                Field field = (Field) left;
                if (field.path() instanceof ColumnIdent && columnsToFind.contains(field.path())) {
                    currentOrBranch.pairs.add(new EqualPair((ColumnIdent) field.path(), fun));
                    return;
                }
            }
            seenOther = true;
        }

        void processOrArgument() {
            if (!currentOrBranch.pairs.isEmpty()) {
                currentOrBranch = new Match();
                comparisons.add(currentOrBranch);
            }
        }

        List<Match> generateMatches() {
            if (seenOther) {
                return Collections.emptyList();
            }
            ArrayList<Match> result = new ArrayList<>();
            for (Match comparison : comparisons) {
                if (comparison.pairs.size() == columnsToFind.size()) {
                    result.add(comparison);
                } else {
                    return Collections.emptyList();
                }
            }
            return result;
        }
    }

    private static class Visitor extends SymbolVisitor<ComparisonBuilder, Void> {

        static final Visitor INSTANCE = new Visitor();

        @Override
        public Void visitFunction(Function fun, ComparisonBuilder builder) {
            if (builder.seenOther) {
                return null;
            }
            String funName = fun.info().ident().name();
            if (EqOperator.NAME.equals(funName)) {
                builder.registerEquals(fun);
            } else if (AnyEqOperator.NAME.equals(funName)) {
                builder.registerEquals(fun);
            } else if (OrOperator.NAME.equals(funName)) {
                for (Symbol arg : fun.arguments()) {
                    builder.processOrArgument();
                    process(arg, builder);
                }
            } else if (AndOperator.NAME.equals(funName)) {
                for (Symbol arg : fun.arguments()) {
                    process(arg, builder);
                }
            } else {
                // cases like
                //  - not (id = ?)
                //  - id > ?
                // etc.
                builder.seenOther = true;
            }
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, ComparisonBuilder builder) {
            builder.seenOther = true;
            return null;
        }
    }
}
