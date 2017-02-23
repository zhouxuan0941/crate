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

package io.crate.operation.reference.sys.job;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.util.UUID;

public class JobContextLog implements ContextLog {

    private final JobContext jobContext;

    @Nullable
    private final String errorMessage;

    private final long ended;

    public JobContextLog(JobContext jobContext, @Nullable String errorMessage) {
        this.jobContext = jobContext;
        this.errorMessage = errorMessage;
        this.ended = System.currentTimeMillis();
    }

    @VisibleForTesting
    public JobContextLog(JobContext jobContext, @Nullable String errorMessage, long ended) {
        this.jobContext = jobContext;
        this.errorMessage = errorMessage;
        this.ended = ended;
    }

    public UUID id() {
        return jobContext.id;
    }

    public String statement() {
        return jobContext.stmt;
    }

    public long started() {
        return jobContext.started;
    }

    @Override
    public long ended() {
        return ended;
    }

    @Nullable
    public String errorMessage() {
        return errorMessage;
    }

}
