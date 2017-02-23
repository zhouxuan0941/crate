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

package io.crate.testing;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * relation that will return a Reference with Doc granularity / String type for all columns
 */
public class DummyRelation implements AnalyzedRelation {

    private final Set<ColumnIdent> columnReferences = new HashSet<>();

    public DummyRelation(String... referenceNames) {
        for (String referenceName : referenceNames) {
            columnReferences.add(ColumnIdent.fromPath(referenceName));
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return null;
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (columnReferences.contains(path)) {
            return new Field(this, path, DataTypes.STRING);
        }
        return null;
    }

    @Override
    public List<Field> fields() {
        return null;
    }

    @Override
    public QualifiedName getQualifiedName() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        throw new UnsupportedOperationException("method not supported");
    }
}
