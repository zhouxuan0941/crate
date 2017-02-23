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

import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;

import java.util.Optional;

public class AlterTableAnalyzedStatement implements DDLStatement {


    private final DocTableInfo tableInfo;
    private final PartitionName partitionName;
    private final TableParameter tableParameter;
    private final boolean excludePartitions;

    public AlterTableAnalyzedStatement(DocTableInfo tableInfo,
                                       PartitionName partitionName,
                                       TableParameter tableParameter,
                                       boolean excludePartitions) {
        this.tableInfo = tableInfo;
        this.partitionName = partitionName;
        this.tableParameter = tableParameter;
        this.excludePartitions = excludePartitions;
    }

    public DocTableInfo table() {
        return tableInfo;
    }

    public Optional<PartitionName> partitionName() {
        return Optional.ofNullable(partitionName);
    }

    public boolean excludePartitions() {
        return excludePartitions;
    }

    public TableParameter tableParameter() {
        return tableParameter;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitAlterTableStatement(this, context);
    }
}
