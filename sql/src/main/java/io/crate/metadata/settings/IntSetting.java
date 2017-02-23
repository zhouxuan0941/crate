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

import javax.annotation.Nonnull;

public class IntSetting extends Setting<Integer, Integer> {

    private final String name;
    private final Integer defaultValue;
    private final boolean isRuntimeSetting;
    private Integer minValue = Integer.MIN_VALUE;
    private Integer maxValue = Integer.MAX_VALUE;

    public IntSetting(String name, Integer defaultValue, boolean isRuntimeSetting) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.isRuntimeSetting = isRuntimeSetting;
    }

    public IntSetting(String name, Integer defaultValue, boolean isRuntimeSetting, Integer minValue, Integer maxValue) {
        this(name, defaultValue, isRuntimeSetting);
        if (minValue != null) {
            this.minValue = minValue;
        }
        if (maxValue != null) {
            this.maxValue = maxValue;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Integer defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isRuntime() {
        return isRuntimeSetting;
    }

    public Integer maxValue() {
        return this.maxValue;
    }

    public Integer minValue() {
        return this.minValue;
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    public Integer extract(Settings settings) {
        return settings.getAsInt(settingName(), defaultValue());
    }

    public Integer extract(Settings settings, @Nonnull Integer defaultValue) {
        return settings.getAsInt(settingName(), defaultValue);
    }
}
