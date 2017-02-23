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

package io.crate.protocols.postgres;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analyzer;
import io.crate.executor.Executor;

abstract class AbstractPortal implements Portal {

    protected final String name;
    final PortalContext portalContext;
    final SessionContext sessionContext;
    boolean synced = false;

    AbstractPortal(String name, Analyzer analyzer, Executor executor, boolean isReadOnly, SessionContext sessionContext) {
        this.name = name;
        this.sessionContext = sessionContext;
        portalContext = new PortalContext(analyzer, executor, isReadOnly);
    }

    AbstractPortal(String name, SessionContext sessionContext, PortalContext portalContext) {
        this.name = name;
        this.portalContext = portalContext;
        this.sessionContext = sessionContext;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean synced() {
        return synced;
    }

    @Override
    public String toString() {
        return "name: " + name + ", type: " + getClass().getSimpleName();
    }

    static class PortalContext {

        private final Analyzer analyzer;
        private final Executor executor;
        private final boolean isReadOnly;

        private PortalContext(Analyzer analyzer, Executor executor, boolean isReadOnly) {
            this.analyzer = analyzer;
            this.executor = executor;
            this.isReadOnly = isReadOnly;
        }

        Analyzer getAnalyzer() {
            return analyzer;
        }

        Executor getExecutor() {
            return executor;
        }

        boolean isReadOnly() {
            return isReadOnly;
        }
    }
}
