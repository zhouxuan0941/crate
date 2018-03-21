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

package io.crate.analysis.tree;

import io.crate.sql.tree.SortItem;

public class TypedSortItem {

    private final TypedExpression sortKey;
    private final SortItem.Ordering ordering;
    private final SortItem.NullOrdering nullOrdering;

    public TypedSortItem(TypedExpression sortKey, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering) {
        this.sortKey = sortKey;
        this.ordering = ordering;
        this.nullOrdering = nullOrdering;
    }
}
