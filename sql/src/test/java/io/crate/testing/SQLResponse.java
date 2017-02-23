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

import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class SQLResponse {

    private String[] cols;
    private DataType[] colTypes;
    private Object[][] rows;
    private long rowCount;

    SQLResponse(String[] cols,
                Object[][] rows,
                DataType[] colTypes,
                long rowCount) {
        assert cols.length == colTypes.length : "cols and colTypes differ";
        this.cols = cols;
        this.colTypes = colTypes;
        this.rows = rows;
        this.rowCount = rowCount;
    }

    public String[] cols() {
        return cols;
    }

    public DataType[] columnTypes() {
        return colTypes;
    }

    public Object[][] rows() {
        return rows;
    }

    public long rowCount() {
        return rowCount;
    }

    private static String arrayToString(@Nullable Object[] array) {
        return array == null ? null : Arrays.toString(array);
    }

    @Override
    public String toString() {
        return "SQLResponse{" +
               "cols=" + arrayToString(cols()) +
               "colTypes=" + arrayToString(columnTypes()) +
               ", rows=" + ((rows != null) ? rows.length : -1) +
               ", rowCount=" + rowCount +
               '}';
    }
}
