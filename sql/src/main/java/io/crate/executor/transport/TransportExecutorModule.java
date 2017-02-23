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

package io.crate.executor.transport;

import io.crate.action.job.ContextPreparer;
import io.crate.action.job.TransportJobAction;
import io.crate.executor.Executor;
import io.crate.executor.transport.distributed.TransportDistributedResultAction;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.lucene.LuceneQueryBuilder;
import org.elasticsearch.common.inject.AbstractModule;

public class TransportExecutorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransportActionProvider.class).asEagerSingleton();
        bind(Executor.class).to(TransportExecutor.class).asEagerSingleton();
        bind(ContextPreparer.class).asEagerSingleton();
        bind(LuceneQueryBuilder.class).asEagerSingleton();

        bind(TransportJobAction.class).asEagerSingleton();
        bind(TransportDistributedResultAction.class).asEagerSingleton();
        bind(TransportShardUpsertAction.class).asEagerSingleton();
        bind(TransportShardDeleteAction.class).asEagerSingleton();
        bind(TransportFetchNodeAction.class).asEagerSingleton();
        bind(TransportKillAllNodeAction.class).asEagerSingleton();
        bind(TransportKillJobsNodeAction.class).asEagerSingleton();
        bind(TransportNodeStatsAction.class).asEagerSingleton();
    }
}
