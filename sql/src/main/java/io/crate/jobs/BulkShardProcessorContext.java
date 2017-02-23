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

package io.crate.jobs;

import io.crate.executor.transport.ShardRequest;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BulkShardProcessorContext extends AbstractExecutionSubContext {

    private static final ESLogger LOGGER = Loggers.getLogger(BulkShardProcessorContext.class);

    private final BulkShardProcessor<? extends ShardRequest> bulkShardProcessor;

    public BulkShardProcessorContext(int id, BulkShardProcessor<? extends ShardRequest> bulkShardProcessor) {
        super(id, LOGGER);
        this.bulkShardProcessor = bulkShardProcessor;
    }

    @Override
    protected void innerStart() {
        bulkShardProcessor.close();
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        bulkShardProcessor.kill(t);
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (t != null) {
            bulkShardProcessor.kill(t);
        }
    }

    public boolean add(String indexName,
                       ShardRequest.Item item,
                       @Nullable String routing) {
        return bulkShardProcessor.add(indexName, item, routing);
    }

    @Override
    public String name() {
        return "bulk-update-by-id";
    }
}
