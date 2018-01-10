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
import io.crate.data.Row;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

public class InnerJoinSortedMergeBatchIterator<L, R, C> extends NestedLoopBatchIterator<L, R, C> {

    private static final Predicate<Row> greaterThan = row -> (Integer)row.get(0) < (Integer)row.get(1);
    final Predicate<C> joinCondition;

    InnerJoinSortedMergeBatchIterator(BatchIterator<L> left,
                                      BatchIterator<R> right,
                                      ElementCombiner<L, R, C> combiner,
                                      Predicate<C> joinCondition) {
        super(left, right, combiner);
        this.joinCondition = joinCondition;
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
        if (activeIt == left) {
            if (this.left.moveNext() == false) {
                return false;
            }
            combiner.setLeft(this.left.currentElement());
        }

        activeIt = right;
        while (right.moveNext()) {
            combiner.setRight(right.currentElement());
            if (joinCondition.test(combiner.currentElement())) {
                return true;
            }
            if (greaterThan.test((Row) combiner.currentElement())) {
                right.moveToStart();
                if (left.moveNext() == false) {
                    activeIt = left;
                    return false;
                }
                combiner.setLeft(left.currentElement());
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
