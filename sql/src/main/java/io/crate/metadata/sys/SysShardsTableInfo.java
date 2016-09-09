/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SysShardsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "shards");
    private final ClusterService service;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent NUM_DOCS = new ColumnIdent("num_docs");
        public static final ColumnIdent PRIMARY = new ColumnIdent("primary");
        public static final ColumnIdent RELOCATING_NODE = new ColumnIdent("relocating_node");
        public static final ColumnIdent SIZE = new ColumnIdent("size");
        public static final ColumnIdent STATE = new ColumnIdent("state");
        public static final ColumnIdent ROUTING_STATE = new ColumnIdent("routing_state");
        public static final ColumnIdent ORPHAN_PARTITION = new ColumnIdent("orphan_partition");

        public static final ColumnIdent RECOVERY = new ColumnIdent("recovery");
        public static final ColumnIdent RECOVERY_STAGE = new ColumnIdent("recovery", ImmutableList.of("stage"));
        public static final ColumnIdent RECOVERY_TYPE = new ColumnIdent("recovery", ImmutableList.of("type"));
        public static final ColumnIdent RECOVERY_TOTAL_TIME =
                new ColumnIdent("recovery", ImmutableList.of("total_time"));

        public static final ColumnIdent RECOVERY_FILES = new ColumnIdent("recovery", ImmutableList.of("files"));
        public static final ColumnIdent RECOVERY_FILES_USED =
                new ColumnIdent("recovery", ImmutableList.of("files", "used"));
        public static final ColumnIdent RECOVERY_FILES_REUSED =
                new ColumnIdent("recovery", ImmutableList.of("files", "reused"));
        public static final ColumnIdent RECOVERY_FILES_RECOVERED =
                new ColumnIdent("recovery", ImmutableList.of("files", "recovered"));
        public static final ColumnIdent RECOVERY_FILES_PERCENT =
                new ColumnIdent("recovery", ImmutableList.of("files", "percent"));

        public static final ColumnIdent RECOVERY_SIZE =
                new ColumnIdent("recovery", ImmutableList.of("size"));
        public static final ColumnIdent RECOVERY_SIZE_USED =
                new ColumnIdent("recovery", ImmutableList.of("size", "used"));
        public static final ColumnIdent RECOVERY_SIZE_REUSED =
                new ColumnIdent("recovery", ImmutableList.of("size", "reused"));
        public static final ColumnIdent RECOVERY_SIZE_RECOVERED =
                new ColumnIdent("recovery", ImmutableList.of("size", "recovered"));
        public static final ColumnIdent RECOVERY_SIZE_PERCENT =
                new ColumnIdent("recovery", ImmutableList.of("size", "percent"));
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(IDENT, columnIdent, RowGranularity.SHARD, dataType);
    }

    public static class Refs {
        public static final Reference ID = createRef(Columns.ID, DataTypes.INTEGER);
        public static final Reference SCHEMA_NAME = createRef(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        public static final Reference PARTITION_IDENT = createRef(Columns.PARTITION_IDENT, DataTypes.STRING);
        public static final Reference NUM_DOCS = createRef(Columns.NUM_DOCS, DataTypes.LONG);
        public static final Reference PRIMARY = createRef(Columns.PRIMARY, DataTypes.BOOLEAN);
        public static final Reference RELOCATING_NODE = createRef(Columns.RELOCATING_NODE, DataTypes.STRING);
        public static final Reference SIZE = createRef(Columns.SIZE, DataTypes.LONG);
        public static final Reference STATE = createRef(Columns.STATE, DataTypes.STRING);
        public static final Reference ROUTING_STATE = createRef(Columns.ROUTING_STATE, DataTypes.STRING);
        public static final Reference ORPHAN_PARTITION = createRef(Columns.ORPHAN_PARTITION, DataTypes.BOOLEAN);
        public static final Reference RECOVERY = createRef(Columns.RECOVERY, DataTypes.OBJECT);
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(
            Columns.SCHEMA_NAME,
            Columns.TABLE_NAME,
            Columns.ID,
            Columns.PARTITION_IDENT
    );

    private final TableColumn nodesTableColumn;

    @Inject
    public SysShardsTableInfo(ClusterService service, SysNodesTableInfo sysNodesTableInfo) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.SHARD)
                        .register(Columns.SCHEMA_NAME, StringType.INSTANCE)
                        .register(Columns.TABLE_NAME, StringType.INSTANCE)
                        .register(Columns.ID, IntegerType.INSTANCE)
                        .register(Columns.PARTITION_IDENT, StringType.INSTANCE)
                        .register(Columns.NUM_DOCS, LongType.INSTANCE)
                        .register(Columns.PRIMARY, BooleanType.INSTANCE)
                        .register(Columns.RELOCATING_NODE, StringType.INSTANCE)
                        .register(Columns.SIZE, LongType.INSTANCE)
                        .register(Columns.STATE, StringType.INSTANCE)
                        .register(Columns.ROUTING_STATE, StringType.INSTANCE)
                        .register(Columns.ORPHAN_PARTITION, BooleanType.INSTANCE)

                        .register(Columns.RECOVERY, ObjectType.INSTANCE)
                        .register(Columns.RECOVERY_STAGE, StringType.INSTANCE)
                        .register(Columns.RECOVERY_TYPE, StringType.INSTANCE)
                        .register(Columns.RECOVERY_TOTAL_TIME, LongType.INSTANCE)

                        .register(Columns.RECOVERY_SIZE, ObjectType.INSTANCE)
                        .register(Columns.RECOVERY_SIZE_USED, LongType.INSTANCE)
                        .register(Columns.RECOVERY_SIZE_REUSED, LongType.INSTANCE)
                        .register(Columns.RECOVERY_SIZE_RECOVERED, LongType.INSTANCE)
                        .register(Columns.RECOVERY_SIZE_PERCENT, FloatType.INSTANCE)

                        .register(Columns.RECOVERY_FILES, ObjectType.INSTANCE)
                        .register(Columns.RECOVERY_FILES_USED, IntegerType.INSTANCE)
                        .register(Columns.RECOVERY_FILES_REUSED, IntegerType.INSTANCE)
                        .register(Columns.RECOVERY_FILES_RECOVERED, IntegerType.INSTANCE)
                        .register(Columns.RECOVERY_FILES_PERCENT, FloatType.INSTANCE)
                        .putInfoOnly(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT)),
                PRIMARY_KEY);
        this.service = service;
        nodesTableColumn = sysNodesTableInfo.tableColumn();
    }

    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        Reference info = super.getReference(columnIdent);
        if (info == null) {
            return nodesTableColumn.getReference(this.ident(), columnIdent);
        }
        return info;
    }

    private void processShardRouting(Map<String, Map<String, List<Integer>>> routing, ShardRouting shardRouting, ShardId shardId) {
        String node;
        int id;
        String index = shardId.getIndex();

        if (shardRouting == null) {
            node = service.localNode().id();
            id = UnassignedShard.markUnassigned(shardId.id());
        } else {
            node = shardRouting.currentNodeId();
            id = shardRouting.id();
        }
        Map<String, List<Integer>> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            routing.put(node, nodeMap);
        }

        List<Integer> shards = nodeMap.get(index);
        if (shards == null) {
            shards = new ArrayList<>();
            nodeMap.put(index, shards);
        }
        shards.add(id);
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.SHARD;
    }

    /**
     * Retrieves the routing for sys.shards
     *
     * This routing contains ALL shards of ALL indices.
     * Any shards that are not yet assigned to a node will have a NEGATIVE shard id (see {@link UnassignedShard}
     */
    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        // TODO: filter on whereClause
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        ClusterState state = service.state();
        String[] concreteIndices = state.metaData().concreteAllIndices();
        GroupShardsIterator groupShardsIterator = state.getRoutingTable().allAssignedShardsGrouped(concreteIndices, true, true);
        for (final ShardIterator shardIt : groupShardsIterator) {
            final ShardRouting shardRouting = shardIt.nextOrNull();
            processShardRouting(locations, shardRouting, shardIt.shardId());
        }
        return new Routing(locations);
    }
}
