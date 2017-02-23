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

package io.crate.operation.reference.sys.check;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;

public abstract class AbstractSysNodeCheck extends AbstractSysCheck implements SysNodeCheck {

    private static final String LINK_PATTERN = "https://cr8.is/d-node-check-";
    protected final ClusterService clusterService;

    private BytesRef nodeId;
    private boolean acknowledged;

    public AbstractSysNodeCheck(int id, String description, Severity severity, ClusterService clusterService) {
        super(id, description, severity, LINK_PATTERN);
        this.clusterService = clusterService;
        acknowledged = false;
    }

    @Override
    public BytesRef nodeId() {
        if (nodeId == null) {
            nodeId = new BytesRef(clusterService.localNode().getId());
        }
        return nodeId;
    }

    @Override
    public boolean acknowledged() {
        return acknowledged;
    }

    @Override
    public void acknowledged(boolean value) {
        acknowledged = value;
    }
}
