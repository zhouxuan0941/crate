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

package io.crate.metadata.shard.blob;

import io.crate.blob.v2.BlobShard;
import io.crate.metadata.AbstractReferenceResolver;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.operation.reference.sys.shard.*;
import io.crate.operation.reference.sys.shard.blob.BlobShardBlobPathExpression;
import io.crate.operation.reference.sys.shard.blob.BlobShardNumDocsExpression;
import io.crate.operation.reference.sys.shard.blob.BlobShardSizeExpression;
import io.crate.operation.reference.sys.shard.blob.BlobShardTableNameExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

public class BlobShardReferenceResolver extends AbstractReferenceResolver {

    public BlobShardReferenceResolver(BlobShard blobShard) {
        IndexShard indexShard = blobShard.indexShard();
        ShardId shardId = indexShard.shardId();
        implementations.put(SysShardsTableInfo.ReferenceIdents.ID, new LiteralReferenceImplementation<>(shardId.id()));
        implementations.put(SysShardsTableInfo.ReferenceIdents.NUM_DOCS, new BlobShardNumDocsExpression(blobShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PRIMARY, new ShardPrimaryExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.RELOCATING_NODE,
            new ShardRelocatingNodeExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.SCHEMA_NAME,
            new LiteralReferenceImplementation<>(new BytesRef(BlobSchemaInfo.NAME)));
        implementations.put(SysShardsTableInfo.ReferenceIdents.SIZE, new BlobShardSizeExpression(blobShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.STATE, new ShardStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.ROUTING_STATE, new ShardRoutingStateExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.TABLE_NAME, new BlobShardTableNameExpression(shardId));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PARTITION_IDENT,
            new LiteralReferenceImplementation<>(new BytesRef("")));
        implementations.put(SysShardsTableInfo.ReferenceIdents.ORPHAN_PARTITION,
            new LiteralReferenceImplementation<>(false));
        implementations.put(SysShardsTableInfo.ReferenceIdents.PATH, new ShardPathExpression(indexShard));
        implementations.put(SysShardsTableInfo.ReferenceIdents.BLOB_PATH, new BlobShardBlobPathExpression(blobShard));
    }
}
