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

package io.crate.metadata.settings;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import javax.annotation.Nullable;

public class MemorySetting extends Setting<String, ByteSizeValue> {

    private final String name;
    private final String defaultValue;
    private final boolean isRuntime;
    private final Setting<?, ?> parent;

    public MemorySetting(String name, String defaultValue, boolean isRuntime, @Nullable Setting<?, ?> parent) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.isRuntime = isRuntime;
        this.parent = parent;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public ByteSizeValue extract(Settings settings) {
        return settings.getAsMemory(settingName(), defaultValue);
    }

    @Override
    public boolean isRuntime() {
        return isRuntime;
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING;
    }

    @Override
    public Setting parent() {
        return parent;
    }
}
