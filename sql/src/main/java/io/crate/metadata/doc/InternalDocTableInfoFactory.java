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

package io.crate.metadata.doc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.TableParameterInfo;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.table.Operation;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

@Singleton
public class InternalDocTableInfoFactory implements DocTableInfoFactory {

    private final Functions functions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider;
    private final ExecutorService executorService;
    private final ClusterService clusterService;

    @Inject
    public InternalDocTableInfoFactory(ClusterService clusterService,
                                       Functions functions,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider,
                                       ThreadPool threadPool) {
        this(clusterService,
            functions,
            indexNameExpressionResolver,
            putIndexTemplateActionProvider,
            (ExecutorService) threadPool.executor(ThreadPool.Names.SUGGEST));
    }

    @VisibleForTesting
    InternalDocTableInfoFactory(ClusterService clusterService,
                                Functions functions,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider,
                                ExecutorService executorService) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        putIndexTemplateActionProvider = transportPutIndexTemplateActionProvider;
        this.executorService = executorService;
    }

    @Override
    public DocTableInfo create(TableIdent ident, ClusterService clusterService) {
        MetaData metaData = clusterService.state().metaData();
        boolean checkAliasSchema = metaData.settings().getAsBoolean("crate.table_alias.schema_check", true);
        try {
            return fromMetaData(ident, metaData.templates(), metaData.indices());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


    public DocTableInfo fromMetaData(TableIdent tableIdent,
                                     ImmutableOpenMap<String, IndexTemplateMetaData> templates,
                                     ImmutableOpenMap<String, IndexMetaData> indices) throws IOException {
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
        IndexTemplateMetaData templateMetaData = templates.get(templateName);
        if (templateMetaData == null) {
            IndexMetaData indexMetaData = indices.get(tableIdent.indexName());
            if (indexMetaData == null) {
                throw new TableUnknownException(tableIdent);
            }
            return fromIndexMetaData(tableIdent, indexMetaData);
        }
        return fromTemplate(tableIdent, templateMetaData);
    }

    private DocTableInfo fromIndexMetaData(TableIdent tableIdent, IndexMetaData indexMetaData) throws IOException {
        //DocTable docTable = DocTable.fromIndexMetaData(tableIdent, indexMetaData);
        Settings settings = indexMetaData.getSettings();
        MappingMetaData mapping = indexMetaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        MappingParser.Context context = MappingParser.parse(mapping);

        ImmutableMap<ColumnIdent, Reference> references = context.createReferences(tableIdent);
        return new DocTableInfo(
            tableIdent,
            new ArrayList<>(references.values()),
            context.partitionedByColumns(),
            context.generatedColumns(),
            context.indexColumns(),
            references,
            ImmutableMap.of(), // TODO: analyzers,
            context.primaryKeys(),
            context.clusteredBy(),
            false, //isAlias,
            false, // hasAutoGeneratedPrimaryKey,
            new String[] { tableIdent.indexName() }, // concreteIndices
            clusterService,
            indexNameExpressionResolver,
            indexMetaData.getNumberOfShards(),
            NumberOfReplicas.fromSettings(settings),
            TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData),
            context.partitionedBy(),
            Collections.emptyList(), // partitions
            context.columnPolicy(),
            Operation.buildFromIndexSettings(settings),
            executorService
        );
    }

    private static DocTableInfo fromTemplate(TableIdent tableIdent, IndexTemplateMetaData templateMetaData) {
        return null;
    }
}
