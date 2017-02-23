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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.sql.tree.OptimizeStatement;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

import static io.crate.analyze.OptimizeSettings.*;

class OptimizeTableAnalyzer {

    private static final ImmutableMap<String, SettingsApplier> SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(MAX_NUM_SEGMENTS.name(), new SettingsAppliers.IntSettingsApplier(MAX_NUM_SEGMENTS))
        .put(ONLY_EXPUNGE_DELETES.name(), new SettingsAppliers.BooleanSettingsApplier(ONLY_EXPUNGE_DELETES))
        .put(FLUSH.name(), new SettingsAppliers.BooleanSettingsApplier(FLUSH))
        .build();

    private final Schemas schemas;

    OptimizeTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public OptimizeTableAnalyzedStatement analyze(OptimizeStatement stmt, Analysis analysis) {
        Set<String> indexNames = TableAnalyzer.getIndexNames(
            stmt.tables(), schemas, analysis.parameterContext(), analysis.sessionContext().defaultSchema());

        // validate and extract settings
        Settings.Builder builder = GenericPropertiesConverter.settingsFromProperties(
            stmt.properties(), analysis.parameterContext(), SETTINGS);
        Settings settings = builder.build();
        return new OptimizeTableAnalyzedStatement(indexNames, settings);
    }
}
