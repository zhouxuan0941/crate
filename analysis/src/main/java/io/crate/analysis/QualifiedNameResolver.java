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

import io.crate.analysis.tree.AnalysedRelation;
import io.crate.analysis.tree.TypedExpression;
import io.crate.sql.tree.QualifiedName;

import java.util.List;

public final class QualifiedNameResolver {

    private final List<AnalysedRelation> sources;

    QualifiedNameResolver(List<AnalysedRelation> sources) {
        this.sources = sources;
    }

    public TypedExpression get(QualifiedName name) {
        List<String> parts = name.getParts();
        String schemaName = null;
        String tableName = null;
        final String columnName;
        switch (parts.size()) {
            case 1:
                columnName = parts.get(0);
                break;

            case 2:
                tableName = parts.get(0);
                columnName = parts.get(1);
                break;

            case 3:
                schemaName = parts.get(0);
                tableName = parts.get(1);
                columnName = parts.get(2);

            default:
                throw new IllegalArgumentException("Invalid column name: " + name);
        }
        return sources.get(0).get(columnName);
    }
}
