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

import io.crate.analysis.tree.AnalysedTable;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;

public class MetaData {

    private final Schemas schemas;

    public MetaData(Schemas schemas) {
        this.schemas = schemas;
    }

    @Nullable
    public AnalysedTable get(List<String> searchPath, QualifiedName name) {
        List<String> parts = name.getParts();
        String schemaName = null;
        final String table;
        switch (parts.size()) {
            case 1:
                table = parts.get(0);
                break;

            case 2:
                schemaName = parts.get(0);
                table = parts.get(1);
                break;

            default:
                throw new IllegalArgumentException("Invalid column name: " + name);

        }
        if (schemaName == null) {
            return fromSearchPath(searchPath, table);
        }
        Schema schema = schemas.get(schemaName);
        if (schema == null) {
            return null;
        }
        return schema.get(table);
    }

    private AnalysedTable fromSearchPath(List<String> searchPath, String table) {
        for (String path : searchPath) {
            Schema schema = schemas.get(path);
            if (schema == null) {
                continue;
            }
            AnalysedTable analysedTable = schema.get(table);
            if (analysedTable != null) {
                return analysedTable;
            }
        }
        return null;
    }
}
