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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.executor.transport.ddl.CreateTableRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

@Singleton
public class CreateTableClusterStateExecutor extends DDLClusterStateTaskExecutor<CreateTableRequest> {

    private final Logger logger;
    private final MetaDataCreateIndexService metaDataCreateIndexService;
    private final NamedXContentRegistry xContentRegistry;
    private final IndicesService indicesService;
    private final AliasValidator aliasValidator;
    private final AllocationService allocationService;
    private final IndexScopedSettings indexScopedSettings;
    private final DDLClusterStateService ddlClusterStateService;

    public CreateTableClusterStateExecutor(Settings settings,
                                           MetaDataCreateIndexService metaDataCreateIndexService,
                                           NamedXContentRegistry xContentRegistry,
                                           IndicesService indexServices,
                                           AliasValidator aliasValidator,
                                           AllocationService allocationService,
                                           IndexScopedSettings indexScopedSettings,
                                           DDLClusterStateService ddlClusterStateService) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.metaDataCreateIndexService = metaDataCreateIndexService;
        this.xContentRegistry = xContentRegistry;
        this.indicesService = indexServices;
        this.aliasValidator = aliasValidator;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.ddlClusterStateService = ddlClusterStateService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, CreateTableRequest request) throws Exception {
        if (request.isPartitioned()) {
            currentState = createPartitionedTableTemplate(currentState, request);
        } else {
            currentState = createTable(currentState, request);
        }
        // call possible DDL modifier
        currentState = ddlClusterStateService.onCreateTable(currentState, request.tableIdent());
        return currentState;
    }

    /**
     * Code was copied from {@link MetaDataCreateIndexService#onlyCreateIndex}.
     */
    private ClusterState createTable(ClusterState currentState, CreateTableRequest request) throws Exception {
        Index createdIndex = null;
        String removalExtraInfo = null;
        IndicesClusterStateService.AllocatedIndices.IndexRemovalReason removalReason = IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.FAILURE;
        try {
            String indexName = request.tableIdent().indexName();
            Settings settings = validateAndNormalizeSettings(request.settings());

            validate(indexName, settings, currentState);

            // we only find a template when its an API call (a new index)
            // find templates, highest order are better matching
            List<IndexTemplateMetaData> templates = findTemplates(indexName, currentState);

            Map<String, IndexMetaData.Custom> customs = new HashMap<>();

            // add the request mapping
            Map<String, Map<String, Object>> mappings = new HashMap<>();

            Map<String, AliasMetaData> templatesAliases = new HashMap<>();

            List<String> templateNames = new ArrayList<>();

            for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                mappings.put(entry.getKey(), MapperService.parseMapping(xContentRegistry, entry.getValue()));
            }

            // apply templates, merging the mappings into the request mapping if exists
            for (IndexTemplateMetaData template : templates) {
                templateNames.add(template.getName());
                for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                    String mappingString = cursor.value.string();
                    if (mappings.containsKey(cursor.key)) {
                        XContentHelper.mergeDefaults(mappings.get(cursor.key),
                            MapperService.parseMapping(xContentRegistry, mappingString));
                    } else {
                        mappings.put(cursor.key,
                            MapperService.parseMapping(xContentRegistry, mappingString));
                    }
                }
                // handle custom
                for (ObjectObjectCursor<String, IndexMetaData.Custom> cursor : template.customs()) {
                    String type = cursor.key;
                    IndexMetaData.Custom custom = cursor.value;
                    IndexMetaData.Custom existing = customs.get(type);
                    if (existing == null) {
                        customs.put(type, custom);
                    } else {
                        IndexMetaData.Custom merged = existing.mergeWith(custom);
                        customs.put(type, merged);
                    }
                }
                //handle aliases
                for (ObjectObjectCursor<String, AliasMetaData> cursor : template.aliases()) {
                    AliasMetaData aliasMetaData = cursor.value;
                    //if an alias with same name was already processed, ignore this one
                    if (templatesAliases.containsKey(cursor.key)) {
                        continue;
                    }

                    //Allow templatesAliases to be templated by replacing a token with the name of the index that we are applying it to
                    if (aliasMetaData.alias().contains("{index}")) {
                        String templatedAlias = aliasMetaData.alias().replace("{index}", indexName);
                        aliasMetaData = AliasMetaData.newAliasMetaData(aliasMetaData, templatedAlias);
                    }

                    aliasValidator.validateAliasMetaData(aliasMetaData, indexName, currentState.metaData());
                    templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
                }
            }
            Settings.Builder indexSettingsBuilder = Settings.builder();
            // apply templates, here, in reverse order, since first ones are better matching
            for (int i = templates.size() - 1; i >= 0; i--) {
                indexSettingsBuilder.put(templates.get(i).settings());
            }
            // now, put the request settings, so they override templates
            indexSettingsBuilder.put(settings);
            if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
            }
            if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
            }
            if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
            }

            if (indexSettingsBuilder.get(SETTING_VERSION_CREATED) == null) {
                DiscoveryNodes nodes = currentState.nodes();
                final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
                indexSettingsBuilder.put(SETTING_VERSION_CREATED, createdVersion);
            }

            if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
                indexSettingsBuilder.put(SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
            }
            indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, indexName);
            indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
            int routingNumShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettingsBuilder.build());

            Settings actualIndexSettings = indexSettingsBuilder.build();
            IndexMetaData.Builder tmpImdBuilder = IndexMetaData.builder(indexName)
                .setRoutingNumShards(routingNumShards);
            // Set up everything, now locally create the index to see that things are ok, and apply
            final IndexMetaData tmpImd = tmpImdBuilder.settings(actualIndexSettings).build();
            ActiveShardCount waitForActiveShards = tmpImd.getWaitForActiveShards();
            if (waitForActiveShards.validate(tmpImd.getNumberOfReplicas()) == false) {
                throw new IllegalArgumentException("invalid wait_for_active_shards[" + SETTING_WAIT_FOR_ACTIVE_SHARDS.get(settings) +
                                                   "]: cannot be greater than number of shard copies [" +
                                                   (tmpImd.getNumberOfReplicas() + 1) + "]");
            }
            // create the index here (on the master) to validate it can be created, as well as adding the mapping
            final IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList());
            createdIndex = indexService.index();
            // now add the mappings
            MapperService mapperService = indexService.mapperService();
            try {
                mapperService.merge(mappings, MapperService.MergeReason.MAPPING_UPDATE, true);
            } catch (Exception e) {
                removalExtraInfo = "failed on parsing default mapping/mappings on index creation";
                throw e;
            }

            // the context is only used for validation so it's fine to pass fake values for the shard id and the current
            // timestamp
            final QueryShardContext queryShardContext = indexService.newQueryShardContext(0, null, () -> 0L);
            for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                if (aliasMetaData.filter() != null) {
                    aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(),
                        queryShardContext, xContentRegistry);
                }
            }

            // now, update the mappings with the actual source
            Map<String, MappingMetaData> mappingsMetaData = new HashMap<>();
            for (DocumentMapper mapper : mapperService.docMappers(true)) {
                MappingMetaData mappingMd = new MappingMetaData(mapper);
                mappingsMetaData.put(mapper.type(), mappingMd);
            }

            final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexName)
                .settings(actualIndexSettings)
                .setRoutingNumShards(routingNumShards);
            for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                indexMetaDataBuilder.putMapping(mappingMd);
            }

            for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                indexMetaDataBuilder.putAlias(aliasMetaData);
            }

            for (Map.Entry<String, IndexMetaData.Custom> customEntry : customs.entrySet()) {
                indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
            }

            indexMetaDataBuilder.state(IndexMetaData.State.OPEN);

            final IndexMetaData indexMetaData;
            try {
                indexMetaData = indexMetaDataBuilder.build();
            } catch (Exception e) {
                removalExtraInfo = "failed to build index metadata";
                throw e;
            }

            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetaData.getIndex(),
                indexMetaData.getSettings());

            MetaData newMetaData = MetaData.builder(currentState.metaData())
                .put(indexMetaData, false)
                .build();

            String maybeShadowIndicator = IndexMetaData.isIndexUsingShadowReplicas(indexMetaData.getSettings()) ? "s" : "";
            logger.info("[{}] creating table, templates {}, shards [{}]/[{}{}], mappings {}",
                indexName, templateNames, indexMetaData.getNumberOfShards(),
                indexMetaData.getNumberOfReplicas(), maybeShadowIndicator, mappings.keySet());

            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            blocks.updateBlocks(indexMetaData);

            ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                .addAsNew(updatedState.metaData().index(indexName));
            updatedState = allocationService.reroute(
                ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                "index [" + indexName + "] created");
            removalExtraInfo = "cleaning up after validating index on master";
            removalReason = IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;
            return updatedState;
        } finally {
            if (createdIndex != null) {
                // Index was already partially created - need to clean up
                indicesService.removeIndex(createdIndex, removalReason, removalExtraInfo);
            }
        }
    }

    /**
     * Mostly copied from {@link MetaDataIndexTemplateService#putTemplate(MetaDataIndexTemplateService.PutRequest, MetaDataIndexTemplateService.PutListener)}
     */
    private ClusterState createPartitionedTableTemplate(ClusterState currentState,
                                                        CreateTableRequest request) throws Exception{
        TableIdent tableIdent = request.tableIdent();
        String indexName = tableIdent.indexName();
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());

        if (currentState.metaData().templates().containsKey(templateName)) {
            throw new IllegalArgumentException("index_template [" + templateName + "] already exists");
        }

        final IndexTemplateMetaData.Builder templateBuilder = IndexTemplateMetaData.builder(templateName);

        String templatePrefix = PartitionName.templatePrefix(tableIdent.schema(), tableIdent.name());
        Alias alias = new Alias(indexName);
        Settings settings = validateAndNormalizeSettings(request.settings());
        validateTemplateParameters(templateName, templatePrefix, alias);

        addTemplate(templatePrefix, settings, request.mappings(), templateBuilder);

        AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
            .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).build();
        templateBuilder.putAlias(aliasMetaData);
        IndexTemplateMetaData template = templateBuilder.build();

        MetaData.Builder builder = MetaData.builder(currentState.metaData()).put(template);

        return ClusterState.builder(currentState).metaData(builder).build();
    }

    private void validate(String indexName, Settings settings, ClusterState state) {
        MetaDataCreateIndexService.validateIndexName(indexName, state);
        metaDataCreateIndexService.validateIndexSettings(indexName, settings);
    }

    /**
     * Copied from {@link MetaDataCreateIndexService#findTemplates(CreateIndexClusterStateUpdateRequest, ClusterState)}
     */
    private List<IndexTemplateMetaData> findTemplates(String indexName, ClusterState state) throws IOException {
        List<IndexTemplateMetaData> templates = new ArrayList<>();
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;
            if (Regex.simpleMatch(template.template(), indexName)) {
                templates.add(template);
            }
        }

        CollectionUtil.timSort(templates, Comparator.comparingInt(IndexTemplateMetaData::order).reversed());
        return templates;
    }

    /**
     * Copied from {@link MetaDataIndexTemplateService#validateAndAddTemplate(MetaDataIndexTemplateService.PutRequest, IndexTemplateMetaData.Builder, IndicesService, NamedXContentRegistry)}
     */
    private void addTemplate(String templatePrefix,
                             Settings settings,
                             Map<String, String> mappings,
                             IndexTemplateMetaData.Builder templateBuilder) throws Exception {
        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {
            // use the provided values, otherwise just pick valid dummy values
            int dummyPartitionSize = IndexMetaData.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            int dummyShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);

            //create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(settings)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, dummyShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetaData tmpIndexMetadata = IndexMetaData.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(tmpIndexMetadata, Collections.emptyList());
            createdIndex = dummyIndexService.index();

            templateBuilder.order(100);
            templateBuilder.template(templatePrefix);
            templateBuilder.settings(settings);

            Map<String, Map<String, Object>> mappingsForValidation = new HashMap<>();
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                try {
                    templateBuilder.putMapping(entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
                }
                mappingsForValidation.put(entry.getKey(), MapperService.parseMapping(xContentRegistry, entry.getValue()));
            }

            dummyIndexService.mapperService().merge(mappingsForValidation, MapperService.MergeReason.MAPPING_UPDATE, false);

        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(createdIndex, NO_LONGER_ASSIGNED, " created for parsing template mapping");
            }
        }
    }

    private Settings validateAndNormalizeSettings(Settings settings) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(settings).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(updatedSettingsBuilder);
        return updatedSettingsBuilder.build();
    }

    /**
     * Copied from {@link MetaDataIndexTemplateService#validate(MetaDataIndexTemplateService.PutRequest)}
     */
    private void validateTemplateParameters(String templateName, String templatePrefix, Alias alias) {

        List<String> validationErrors = new ArrayList<>();
        if (templateName.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (templateName.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (templateName.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (templateName.startsWith("_")) {
            validationErrors.add("name must not start with '_'");
        }
        if (!templateName.toLowerCase(Locale.ROOT).equals(templateName)) {
            validationErrors.add("name must be lower cased");
        }
        if (templatePrefix.contains(" ")) {
            validationErrors.add("template must not contain a space");
        }
        if (templatePrefix.contains(",")) {
            validationErrors.add("template must not contain a ','");
        }
        if (templatePrefix.contains("#")) {
            validationErrors.add("template must not contain a '#'");
        }
        if (templatePrefix.startsWith("_")) {
            validationErrors.add("template must not start with '_'");
        }
        if (!Strings.validFileNameExcludingAstrix(templatePrefix)) {
            validationErrors.add("template must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }

        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(templateName, validationException.getMessage());
        }

        //we validate the alias only partially, as we don't know yet to which index it'll get applied to
        aliasValidator.validateAliasStandalone(alias);
        if (templatePrefix.equals(alias.name())) {
            throw new IllegalArgumentException("Alias [" + alias.name() + "] cannot be the same as the template pattern [" + templatePrefix + "]");
        }
    }
}
