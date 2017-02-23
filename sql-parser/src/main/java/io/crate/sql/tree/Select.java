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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Select
    extends Node {
    private final boolean distinct;
    private final List<SelectItem> selectItems;

    public Select(boolean distinct, List<SelectItem> selectItems) {
        this.distinct = distinct;
        this.selectItems = ImmutableList.copyOf(checkNotNull(selectItems, "selectItems"));
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("distinct", distinct)
            .add("selectItems", selectItems)
            .omitNullValues()
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Select select = (Select) o;

        if (distinct != select.distinct) {
            return false;
        }
        if (!selectItems.equals(select.selectItems)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (distinct ? 1 : 0);
        result = 31 * result + selectItems.hashCode();
        return result;
    }
}
