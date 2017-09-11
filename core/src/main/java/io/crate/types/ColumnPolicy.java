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

package io.crate.types;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;

public enum ColumnPolicy {
    DYNAMIC(true),
    STRICT("strict"),
    IGNORED(false);

    public static final String ES_MAPPING_NAME = "dynamic";

    private static final ImmutableList<ColumnPolicy> VALUES = ImmutableList.copyOf(values());

    private final Object mappingValue;

    ColumnPolicy(Object mappingValue) {
        this.mappingValue = mappingValue;
    }

    public String value() {
        return this.name().toLowerCase(Locale.ENGLISH);
    }

    /**
     * get a column policy by its name (case insensitive)
     */
    public static ColumnPolicy byName(String name) {
        return ColumnPolicy.valueOf(name.toUpperCase(Locale.ENGLISH));
    }

    /**
     * get a column policy by its mapping value (true, false or 'strict')
     */
    public static ColumnPolicy of(@Nullable Object dynamic) {
        return of(String.valueOf(dynamic));
    }

    public static ColumnPolicy of(String dynamic) {
        if (Booleans.isExplicitTrue(dynamic)) {
            return DYNAMIC;
        }
        if (Booleans.isExplicitFalse(dynamic)) {
            return IGNORED;
        }
        if (dynamic.equalsIgnoreCase("strict")) {
            return STRICT;
        }
        return DYNAMIC;
    }

    public static ColumnPolicy fromStream(StreamInput in) throws IOException {
        return VALUES.get(in.readVInt());
    }

    public static void toStream(ColumnPolicy columnPolicy, StreamOutput out) throws IOException {
        out.writeVInt(columnPolicy.ordinal());
    }

    /**
     * returns the value to be used in an ES index mapping
     * for the <code>dynamic</code> field
     */
    public Object mappingValue() {
        return mappingValue;
    }
}
