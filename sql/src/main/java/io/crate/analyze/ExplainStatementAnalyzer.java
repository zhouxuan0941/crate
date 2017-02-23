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

package io.crate.analyze;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.*;

public class ExplainStatementAnalyzer {

    private final Analyzer analyzer;

    public ExplainStatementAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public ExplainAnalyzedStatement analyze(Explain node, Analysis analysis) {
        CHECK_VISITOR.process(node.getStatement(), null);
        AnalyzedStatement subStatement = analyzer.analyzedStatement(node.getStatement(), analysis);
        String columnName = SqlFormatter.formatSql(node);
        ExplainAnalyzedStatement explainAnalyzedStatement = new ExplainAnalyzedStatement(columnName, subStatement);
        analysis.rootRelation(explainAnalyzedStatement);
        return explainAnalyzedStatement;
    }

    private static final AstVisitor<Void, Void> CHECK_VISITOR = new AstVisitor<Void, Void>() {

        @Override
        protected Void visitQuery(Query node, Void context) {
            return null;
        }

        @Override
        public Void visitCopyFrom(CopyFrom node, Void context) {
            return null;
        }

        @Override
        protected Void visitNode(Node node, Void context) {
            throw new UnsupportedFeatureException("EXPLAIN is not supported for " + node);
        }
    };

}
