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

package io.crate.analysis;

import io.crate.analysis.tree.AliasedAnalysedRelation;
import io.crate.analysis.tree.AliasedExpression;
import io.crate.analysis.tree.AnalysedQuery;
import io.crate.analysis.tree.AnalysedQuerySpec;
import io.crate.analysis.tree.AnalysedRelation;
import io.crate.analysis.tree.AnalysedStatement;
import io.crate.analysis.tree.AnalysedTable;
import io.crate.analysis.tree.TypedExpression;
import io.crate.analysis.tree.TypedLiteral;
import io.crate.analysis.tree.TypedSortItem;
import io.crate.exceptions.RelationUnknown;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.Relation;
import io.crate.sql.tree.SelectItem;
import io.crate.sql.tree.SingleColumn;
import io.crate.sql.tree.Table;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public final class RelationAnalyser {

    public static AnalysedStatement analyze(MetaData metaData, Session session, Query query) {
        Visitor visitor = new Visitor(session);
        return visitor.process(query, metaData);
    }

    private static class Visitor extends AstVisitor<AnalysedRelation, MetaData> {

        private final Session session;

        Visitor(Session session) {
            this.session = session;
        }

        @Override
        protected AnalysedRelation visitQuery(Query query, MetaData metaData) {
            AnalysedStatement queryBody = process(query.getQueryBody(), metaData);

            List<TypedSortItem> orderBy = emptyList();
            TypedExpression limit = null;
            TypedExpression offset = TypedLiteral.of(0);

            return new AnalysedQuery(queryBody, orderBy, limit, offset);
        }

        @Override
        protected AnalysedRelation visitQuerySpecification(QuerySpecification querySpec, MetaData metaData) {
            List<Relation> from = querySpec.getFrom();
            ArrayList<AnalysedRelation> sources = new ArrayList<>(from.size());
            for (Relation relation : from) {
                sources.add(process(relation, metaData));
            }
            Expressions expressions = new Expressions(new QualifiedNameResolver(sources));

            List<TypedExpression> selectList = getSelectList(querySpec, expressions);
            List<TypedExpression> groupBy = emptyList();
            List<TypedExpression> orderBy = emptyList();
            return new AnalysedQuerySpec(
                querySpec.getSelect().isDistinct(),
                selectList,
                sources,
                querySpec.getWhere().map(expressions::analyse).orElse(null),
                groupBy,
                querySpec.getHaving().map(expressions::analyse).orElse(null),
                orderBy,
                querySpec.getLimit().map(expressions::analyse).orElse(null),
                querySpec.getOffset().map(expressions::analyse).orElse(TypedLiteral.of(0L))
            );
        }

        @Override
        protected AnalysedRelation visitTable(Table table, MetaData metaData) {
            AnalysedTable analysedTable = metaData.get(session.searchPath(), table.getName());
            if (analysedTable == null) {
                 throw new RelationUnknown(table.getName());
            }
            return analysedTable;
        }

        @Override
        protected AnalysedRelation visitAliasedRelation(AliasedRelation node, MetaData metaData) {
            return new AliasedAnalysedRelation(node.getAlias(), process(node.getRelation(), metaData));
        }

        @Override
        protected AnalysedRelation visitNode(Node node, MetaData context) {
            throw new UnsupportedOperationException("NYI: " + node);
        }

        private static List<TypedExpression> getSelectList(QuerySpecification querySpec, Expressions expressions) {
            ArrayList<TypedExpression> selectList = new ArrayList<>();
            for (SelectItem selectItem : querySpec.getSelect().getSelectItems()) {
                if (selectItem instanceof AllColumns) {
                    throw new UnsupportedOperationException("NYI: " + selectItem);
                } else if (selectItem instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) selectItem;
                    TypedExpression expression = expressions.analyse(singleColumn.getExpression());
                    String alias = singleColumn.getAlias();
                    if (alias == null) {
                        selectList.add(expression);
                    } else {
                        selectList.add(new AliasedExpression(alias, expression));
                    }
                } else {
                    throw new IllegalArgumentException("Invalid selectItem: " + selectItem);
                }
            }
            return selectList;
        }
    }
}
