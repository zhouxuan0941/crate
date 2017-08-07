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

package io.crate.metadata.cluster;

import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.cluster.ClusterState;

import javax.annotation.Nullable;

/**
 * Components can implement this interface to hook into DDL statement which are resulting in a changed cluster state.
 * Every implementation must register itself at {@link DDLClusterStateService#addModifier(DDLClusterStateModifier)}.
 *
 * An implementation should return a NULL value if nothing was modified.
 * Otherwise a new {@link ClusterState} object created by the {@link ClusterState.Builder} must be build and returned.
 */
public interface DDLClusterStateModifier {

    /**
     * Called while a table is closed.
     */
    @Nullable
    default ClusterState onCloseTable(ClusterState currentState, TableIdent tableIdent) {
        return null;
    }

    /**
     * Called while a single partition is closed
     */
    @Nullable
    default ClusterState onCloseTablePartition(ClusterState currentState, PartitionName partitionName) {
        return null;
    }

    /**
     * Called while a table is opened.
     */
    @Nullable
    default ClusterState onOpenTable(ClusterState currentState, TableIdent tableIdent) {
        return null;
    }

    /**
     * Called while a single partition is opened.
     */
    @Nullable
    default ClusterState onOpenTablePartition(ClusterState currentState, PartitionName partitionName) {
        return null;
    }

    /**
     * Called while a table is dropped.
     */
    @Nullable
    default ClusterState onDropTable(ClusterState currentState, TableIdent tableIdent) {
        return null;
    }
}
