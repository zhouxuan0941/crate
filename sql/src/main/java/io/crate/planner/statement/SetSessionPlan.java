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

package io.crate.planner.statement;

import io.crate.action.sql.SessionContext;
import io.crate.planner.PlanVisitor;
import io.crate.planner.UnnestablePlan;
import io.crate.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SetSessionPlan extends UnnestablePlan {

    private final UUID id;
    private final Map<String, List<Expression>> settings;
    private final SessionContext sessionContext;

    public SetSessionPlan(UUID id, Map<String, List<Expression>> settings, SessionContext sessionContext) {
        this.id = id;
        this.settings = settings;
        this.sessionContext = sessionContext;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitSetSessionPlan(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    public Map<String, List<Expression>> settings() {
        return settings;
    }

    public SessionContext sessionContext() {
        return sessionContext;
    }
}
