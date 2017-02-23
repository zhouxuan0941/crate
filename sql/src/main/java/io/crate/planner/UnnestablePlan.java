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

package io.crate.planner;

import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;

/**
 * A Plan that can only be used as root plan and cannot be used as sub-plan of another plan.
 */
public abstract class UnnestablePlan implements Plan {

    @Override
    public void addProjection(Projection projection,
                              @Nullable Integer newLimit,
                              @Nullable Integer newOffset,
                              @Nullable Integer newNumOutputs,
                              @Nullable PositionalOrderBy newOrderBy) {
        throw new UnsupportedOperationException("addProjection() is not supported on: " + getClass().getSimpleName());
    }

    @Override
    public ResultDescription resultDescription() {
        throw new UnsupportedOperationException("resultDescription() is not supported on: " + getClass().getSimpleName());
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        throw new UnsupportedOperationException("Cannot change distributionInfo on: " + getClass().getSimpleName());
    }
}
