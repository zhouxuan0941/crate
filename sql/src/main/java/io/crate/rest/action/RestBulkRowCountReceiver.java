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

package io.crate.rest.action;

import io.crate.action.sql.BaseResultReceiver;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class RestBulkRowCountReceiver extends BaseResultReceiver {

    private final Result[] results;
    private final int resultIdx;
    private long rowCount;

    RestBulkRowCountReceiver(Result[] results, int resultIdx) {
        this.results = results;
        this.resultIdx = resultIdx;
    }

    @Override
    public void setNextRow(Row row) {
        rowCount = ((long) row.get(0));
    }

    @Override
    public void allFinished(boolean interrupted) {
        results[resultIdx] = new Result(null, rowCount);
        super.allFinished(interrupted);
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        results[resultIdx] = new Result(Exceptions.messageOf(t), rowCount);
        super.fail(t);
    }

    static class Result {

        private String errorMessage;
        private long rowCount;

        Result(@Nullable String errorMessage, long rowCount) {
            this.errorMessage = errorMessage;
            this.rowCount = rowCount;
        }

        @Nullable
        String errorMessage() {
            return errorMessage;
        }

        long rowCount() {
            return rowCount;
        }
    }
}
