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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class ObjectColumnType extends ColumnType {

    private final Optional<String> objectType;
    private final List<ColumnDefinition> nestedColumns;

    public ObjectColumnType(@Nullable String objectType, @Nullable List<ColumnDefinition> nestedColumns) {
        super("object");
        this.objectType = Optional.ofNullable(objectType);
        this.nestedColumns = MoreObjects.firstNonNull(nestedColumns, ImmutableList.<ColumnDefinition>of());
    }

    public Optional<String> objectType() {
        return objectType;
    }

    public List<ColumnDefinition> nestedColumns() {
        return nestedColumns;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, objectType, nestedColumns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ObjectColumnType that = (ObjectColumnType) o;

        if (!nestedColumns.equals(that.nestedColumns)) return false;
        if (!objectType.equals(that.objectType)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitObjectColumnType(this, context);
    }
}
