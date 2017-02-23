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

package io.crate.data;

public interface Row {


    Row EMPTY = new Row() {

        private final Object[] EMPTY_CELLS = new Object[0];

        @Override
        public int numColumns() {
            return 0;
        }

        @Override
        public Object get(int index) {
            throw new IndexOutOfBoundsException("EMPTY row has no cells");
        }

        @Override
        public Object[] materialize() {
            return EMPTY_CELLS;
        }
    };

    int numColumns();

    /**
     * Returns the element at the specified column
     *
     * @param index index of the column to return
     * @return the value at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    Object get(int index);

    /**
     * Returns a materialized view of this row.
     */
    Object[] materialize();
}
