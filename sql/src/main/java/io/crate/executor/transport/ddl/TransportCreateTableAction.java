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

package io.crate.executor.transport.ddl;

import io.crate.metadata.cluster.CreateTableClusterStateExecutor;
import io.crate.metadata.cluster.DDLClusterStateService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportCreateTableAction extends AbstractDDLTransportAction<CreateTableRequest, CreateTableResponse> {

    private static final String ACTION_NAME = "crate/sql/table/create";

    private final ActiveShardsObserver activeShardsObserver;
    private final CreateTableClusterStateExecutor executor;

    @Inject
    public TransportCreateTableAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      MetaDataCreateIndexService metaDataCreateIndexService,
                                      NamedXContentRegistry xContentRegistry,
                                      IndicesService indexServices,
                                      AliasValidator aliasValidator,
                                      AllocationService allocationService,
                                      IndexScopedSettings indexScopedSettings,
                                      DDLClusterStateService ddlClusterStateService) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, CreateTableRequest::new, CreateTableResponse::new, CreateTableResponse::new,
            "create-table");
        this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
        this.executor = new CreateTableClusterStateExecutor(settings, metaDataCreateIndexService, xContentRegistry,
            indexServices, aliasValidator, allocationService, indexScopedSettings, ddlClusterStateService);
    }

    @Override
    protected void masterOperation(CreateTableRequest request, ClusterState state, ActionListener<CreateTableResponse> listener) throws Exception {
        if (request.isPartitioned() == false) {
            String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.tableIdent().indexName());
            super.masterOperation(request, state, ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    activeShardsObserver.waitForActiveShards(indexName, ActiveShardCount.DEFAULT, request.ackTimeout(),
                        shardsAcked -> {
                            if (shardsAcked == false) {
                                logger.debug("[{}] index created, but the operation timed out while waiting for " +
                                             "enough shards to be started.", indexName);
                            }
                            listener.onResponse(response);
                        }, listener::onFailure);
                } else {
                    listener.onResponse(response);
                }
            }, listener::onFailure));
        } else {
            super.masterOperation(request, state, listener);
        }
    }

    @Override
    public ClusterStateTaskExecutor<CreateTableRequest> clusterStateTaskExecutor(CreateTableRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(CreateTableRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.tableIdent().indexName());
    }
}
