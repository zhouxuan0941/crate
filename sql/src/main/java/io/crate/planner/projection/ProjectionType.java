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

package io.crate.planner.projection;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public enum ProjectionType {

    TOPN(TopNProjection::new),
    GROUP(GroupProjection::new),
    AGGREGATION(AggregationProjection::new),
    MERGE_COUNT_AGGREGATION(i -> MergeCountProjection.INSTANCE),
    FILTER(FilterProjection::new),
    WRITER(WriterProjection::new),
    INDEX_WRITER(SourceIndexWriterProjection::new),
    COLUMN_INDEX_WRITER(ColumnIndexWriterProjection::new),
    UPDATE(UpdateProjection::new),
    SYS_UPDATE(SysUpdateProjection::new),
    DELETE(DeleteProjection::new),
    FETCH(null),
    TOPN_ORDERED(OrderedTopNProjection::new),
    EVAL(EvalProjection::new);

    private final Projection.ProjectionFactory factory;

    ProjectionType(Projection.ProjectionFactory factory) {
        this.factory = factory;
    }

    public Projection newInstance(StreamInput in) throws IOException {
        return factory.newInstance(in);
    }
}
