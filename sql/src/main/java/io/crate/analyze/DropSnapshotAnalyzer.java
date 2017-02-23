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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.executor.transport.RepositoryService;
import io.crate.sql.tree.DropSnapshot;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;

@Singleton
class DropSnapshotAnalyzer {

    private final RepositoryService repositoryService;

    DropSnapshotAnalyzer(RepositoryService repositoryService) {
        this.repositoryService = repositoryService;
    }

    public DropSnapshotAnalyzedStatement analyze(DropSnapshot node) {
        List<String> parts = node.name().getParts();
        Preconditions.checkArgument(parts.size() == 2,
            "Snapshot name not supported, only <repository>.<snapshot> works.");

        String repositoryName = parts.get(0);
        repositoryService.failIfRepositoryDoesNotExist(repositoryName);

        return new DropSnapshotAnalyzedStatement(repositoryName, parts.get(1));
    }
}
