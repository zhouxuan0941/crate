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

package io.crate.executor.task;

import com.google.common.base.Function;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.analyze.AnalyzedStatement;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DDLTask extends JobTask {

    private final AnalyzedStatement analyzedStatement;
    private DDLStatementDispatcher ddlStatementDispatcher;

    public DDLTask(UUID jobId, DDLStatementDispatcher ddlStatementDispatcher, AnalyzedStatement analyzedStatement) {
        super(jobId);
        this.ddlStatementDispatcher = ddlStatementDispatcher;
        this.analyzedStatement = analyzedStatement;
    }

    @Override
    public void execute(final RowReceiver rowReceiver, Row parameters) {
        CompletableFuture<Long> future = ddlStatementDispatcher.dispatch(analyzedStatement, jobId());

        OneRowActionListener<Long> responseOneRowActionListener = new OneRowActionListener<>(rowReceiver, new Function<Long, Row>() {
            @Nullable
            @Override
            public Row apply(@Nullable Long input) {
                return new Row1(input == null ? -1 : input);
            }
        });
        future.whenComplete(responseOneRowActionListener);
    }

}
