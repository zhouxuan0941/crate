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

package io.crate.protocols.postgres;

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class AuthenticationService {

//    private static final Function<String, List<Map<String, Object>>> MAP_PARSER =
//        (String t) -> {
//            try {
//                return XContentType.YAML.xContent().createParser(t).list()
//                    .stream()
//                    .map(x -> (Map<String, Object>) x)
//                    .collect(Collectors.toList());
//            } catch (IOException e) {
//                throw new IllegalArgumentException("Could not parse HBA entry.", e);
//            }
//        };
//
//    public static final CrateSetting SETTING_AUTH_HBA = CrateSetting.of(
//        new Setting<>("auth.host_based", s -> "[]", MAP_PARSER, Setting.Property.NodeScope),
//        DataTypes.OBJECT_ARRAY
//    );

    public static final CrateSetting<List<Map<String,Object>>> SETTING_AUTH_HBA = CrateSetting.of(
        Setting.mapListSetting("auth.host_based", Setting.Property.NodeScope)
    );

}
