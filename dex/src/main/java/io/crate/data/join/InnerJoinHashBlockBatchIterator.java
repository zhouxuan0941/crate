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

package io.crate.data.join;

import io.crate.data.BatchIterator;
import io.crate.data.RowN;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

class InnerJoinHashBlockBatchIterator<L, R, C> extends NestedLoopBatchIterator<L, R, C> {

    final Predicate<C> joinCondition;
    final Map<Integer, L> buffer;

    /**
     * points to the batchIterator which will be used on the next {@link #moveNext()} call
     */
    BatchIterator activeIt;

    InnerJoinHashBlockBatchIterator(BatchIterator<L> left,
                                    BatchIterator<R> right,
                                    ElementCombiner<L, R, C> combiner,
                                    Predicate<C> joinCondition,
                                    int leftSize) {
        super(left, right, combiner);
        this.joinCondition = joinCondition;
        this.buffer = new HashMap<>(leftSize);
        this.activeIt = left;
    }

    @Override
    public C currentElement() {
        return combiner.currentElement();
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = left;
    }

    @Override
    public boolean moveNext() {
        activeIt = left;
        while (this.left.moveNext()) {
            this.buffer.put(Objects.hash(((RowN)this.left.currentElement()).materialize()), (L)((RowN)this.left.currentElement()).materialize());
        }
        if (left.allLoaded()) {
            activeIt = right;

            if (right.moveNext()) {
                int rightHash = Objects.hash(((RowN)this.right.currentElement()).materialize());
                L leftRow = buffer.get(rightHash);
                if (leftRow != null) {
                    combiner.setLeft((L) new RowN((Object[]) leftRow));
                    combiner.setRight(right.currentElement());
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return activeIt.allLoaded();
    }


    @Override
    public void kill(@Nonnull Throwable throwable) {
        left.kill(throwable);
        right.kill(throwable);
    }
}

