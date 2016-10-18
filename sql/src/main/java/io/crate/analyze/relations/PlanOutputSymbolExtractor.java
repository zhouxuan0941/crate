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

package io.crate.analyze.relations;

import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.Projection;

import java.util.List;
import java.util.Locale;

public class PlanOutputSymbolExtractor  {

    private static final Visitor VISITOR = new Visitor();

    public static List<? extends Symbol> extract(Plan plan) {
        return VISITOR.process(plan, null);
    }

    private static class Visitor extends PlanVisitor<Void, List<? extends Symbol>> {
        @Override
        protected List<? extends Symbol> visitPlan(Plan plan, Void context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot extract output symbols of plan '%s'", plan));
        }

        @Override
        public List<? extends Symbol> visitCollectAndMerge(CollectAndMerge plan, Void context) {
            List<Projection> projections = plan.localMerge().projections();
            if (!projections.isEmpty()) {
                return Iterables.getLast(projections).outputs();
            }
            return plan.collectPhase().toCollect();
        }

        @Override
        public List<? extends Symbol> visitNestedLoop(NestedLoop plan, Void context) {
            return Iterables.getLast(plan.nestedLoopPhase().projections()).outputs();
        }

        @Override
        public List<? extends Symbol> visitCountPlan(CountPlan countPlan, Void context) {
            assert countPlan.mergePhase() != null : "expecting mergePhase of CountPlan plan to be set";
            return Iterables.getLast(countPlan.mergePhase().projections()).outputs();
        }

        @Override
        public List<? extends Symbol> visitDistributedGroupBy(DistributedGroupBy plan, Void context) {
            assert plan.localMergePhase() != null : "expecting localMergePhase of DistributedGroupBy plan to be set";
            return Iterables.getLast(plan.localMergePhase().projections()).outputs();
        }
    }
}
