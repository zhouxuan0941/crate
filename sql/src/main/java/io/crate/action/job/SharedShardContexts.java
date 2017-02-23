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

package io.crate.action.job;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;

public class SharedShardContexts {

    private final IndicesService indicesService;
    private final Map<ShardId, SharedShardContext> allocatedShards = new HashMap<>();
    private int readerId = 0;

    public SharedShardContexts(IndicesService indicesService) {
        this.indicesService = indicesService;
    }


    public SharedShardContext createContext(ShardId shardId, int readerId) {
        assert !allocatedShards.containsKey(shardId) : "shardId shouldn't have been allocated yet";
        SharedShardContext sharedShardContext = new SharedShardContext(indicesService, shardId, readerId);
        allocatedShards.put(shardId, sharedShardContext);
        return sharedShardContext;
    }

    public SharedShardContext getOrCreateContext(ShardId shardId) {
        SharedShardContext sharedShardContext = allocatedShards.get(shardId);
        if (sharedShardContext == null) {
            synchronized (this) {
                sharedShardContext = allocatedShards.get(shardId);
                if (sharedShardContext == null) {
                    sharedShardContext = new SharedShardContext(indicesService, shardId, readerId);
                    allocatedShards.put(shardId, sharedShardContext);
                    readerId++;
                }
                return sharedShardContext;
            }
        }
        return sharedShardContext;
    }

    @Override
    public String toString() {
        return "SharedShardContexts{" +
               "allocatedShards=" + allocatedShards.keySet() +
               '}';
    }
}
