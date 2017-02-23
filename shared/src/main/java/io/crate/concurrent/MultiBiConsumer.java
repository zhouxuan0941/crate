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

package io.crate.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * a BiConsumer that can be called multiple times and will call the consumer once numCalls has been made.
 */
public class MultiBiConsumer<T> implements BiConsumer<T, Throwable> {

    private final AtomicInteger counter;
    private final List<T> results;
    private final BiConsumer<List<T>, Throwable> finalConsumer;
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    public MultiBiConsumer(int numCalls, BiConsumer<List<T>, Throwable> finalConsumer) {
        this.finalConsumer = finalConsumer;
        results = new ArrayList<>(numCalls);
        counter = new AtomicInteger(numCalls);
    }

    @Override
    public void accept(T result, Throwable throwable) {
        if (throwable == null) {
            onSuccess(result);
        } else {
            onFailure(throwable);
        }
    }

    private void onSuccess(T result) {
        synchronized (results) {
            results.add(result);
        }
        countdown();
    }

    private void onFailure(Throwable t) {
        lastFailure.set(t);
        countdown();
    }

    private void countdown() {
        if (counter.decrementAndGet() == 0) {
            Throwable throwable = lastFailure.get();
            finalConsumer.accept(results, throwable);
        }
    }
}
