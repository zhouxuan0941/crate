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

package io.crate.operation.projectors;

import io.crate.data.Row1;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

class BulkProcessorFutureCallback implements BiConsumer<BitSet, Throwable> {
    private final AtomicBoolean failed;
    private final RowReceiver rowReceiver;

    public BulkProcessorFutureCallback(AtomicBoolean failed, RowReceiver rowReceiver) {
        this.failed = failed;
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void accept(BitSet bitSet, Throwable t) {
        if (t == null) {
            onSuccess(bitSet);
        } else {
            onFailure(t);
        }
    }

    private void onSuccess(@Nullable BitSet result) {
        if (!failed.get()) {
            long rowCount = result == null ? 0 : result.cardinality();
            rowReceiver.setNextRow(new Row1(rowCount));
            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
        }
    }

    private void onFailure(@Nonnull Throwable t) {
        if (!failed.get()) {
            rowReceiver.fail(t);
        }
    }
}
