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

package io.crate.operation.reference.sys.node;

import org.apache.lucene.util.BytesRef;

class NodeOsInfoStatsExpression extends NestedNodeStatsExpression {

    private static final String AVAILABLE_PROCESSORS = "available_processors";
    private static final String OS = "name";
    private static final String ARCH = "arch";
    private static final String VERSION = "version";
    private static final String JVM = "jvm";

    NodeOsInfoStatsExpression() {
        childImplementations.put(AVAILABLE_PROCESSORS, new SimpleNodeStatsExpression<Integer>() {
            @Override
            public Integer innerValue() {
                return this.row.osInfo().getAvailableProcessors();
            }
        });
        childImplementations.put(OS, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osName();
            }
        });
        childImplementations.put(ARCH, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osArch();
            }
        });
        childImplementations.put(VERSION, new SimpleNodeStatsExpression<BytesRef>() {
            @Override
            public BytesRef innerValue() {
                return this.row.osVersion();
            }
        });
        childImplementations.put(JVM, new NodeOsJvmStatsExpression());
    }
}

