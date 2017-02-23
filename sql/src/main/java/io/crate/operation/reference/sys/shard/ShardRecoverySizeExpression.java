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

package io.crate.operation.reference.sys.shard;

import io.crate.operation.reference.NestedObjectExpression;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.RecoveryState;

public class ShardRecoverySizeExpression extends NestedObjectExpression {

    private static final String USED = "used";
    private static final String REUSED = "reused";
    private static final String RECOVERED = "recovered";
    private static final String PERCENT = "percent";

    public ShardRecoverySizeExpression(IndexShard indexShard) {
        addChildImplementations(indexShard);
    }

    private void addChildImplementations(IndexShard indexShard) {
        childImplementations.put(USED, new ShardRecoveryStateExpression<Long>(indexShard) {
            @Override
            public Long innerValue(RecoveryState recoveryState) {
                return recoveryState.getIndex().totalBytes();
            }
        });
        childImplementations.put(REUSED, new ShardRecoveryStateExpression<Long>(indexShard) {
            @Override
            public Long innerValue(RecoveryState recoveryState) {
                return recoveryState.getIndex().reusedBytes();
            }
        });
        childImplementations.put(RECOVERED, new ShardRecoveryStateExpression<Long>(indexShard) {
            @Override
            public Long innerValue(RecoveryState recoveryState) {
                return recoveryState.getIndex().recoveredBytes();
            }
        });
        childImplementations.put(PERCENT, new ShardRecoveryStateExpression<Float>(indexShard) {
            @Override
            public Float innerValue(RecoveryState recoveryState) {
                return recoveryState.getIndex().recoveredBytesPercent();
            }
        });
    }
}
