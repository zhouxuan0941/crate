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

package io.crate.analyze.relations;

import io.crate.analyze.Fields;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.List;

import static io.crate.analyze.symbol.Symbols.pathFromSymbol;

public class AnalyzedValuesRelation implements QueriedRelation {

    private final List<Symbol> rows;
    private final Fields fields;
    private final QuerySpec qs;
    private QualifiedName qualifiedName = new QualifiedName("VALUES");

    public AnalyzedValuesRelation(List<Symbol> rows) {
        this.rows = rows;
        this.fields = new Fields(rows.size());
        for (Symbol row : rows) {
            Path path = pathFromSymbol(row);
            fields.add(path, new Field(this, path, row.valueType()));
        }
        this.qs = new QuerySpec().outputs(rows);
    }

    @Override
    public QuerySpec querySpec() {
        return qs;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitValues(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation == Operation.READ) {
            return fields.get(path);
        }
        throw new UnsupportedOperationException(operation + " is not supported on " + qualifiedName);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }
}
