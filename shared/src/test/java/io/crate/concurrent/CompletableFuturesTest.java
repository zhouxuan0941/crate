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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;

public class CompletableFuturesTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void failedFutureIsCompletedExceptionally() {
        Exception exception = new Exception("failed future");
        CompletableFuture<Object> failedFuture = CompletableFutures.failedFuture(exception);
        assertThat(failedFuture.isCompletedExceptionally(), Matchers.is(true));
    }

    @Test
    public void testAllAsListFailurePropagation() throws Exception {
        CompletableFuture<Integer> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();
        CompletableFuture<List<Integer>> all = CompletableFutures.allAsList(Arrays.asList(f1, f2));

        f1.completeExceptionally(new IllegalStateException("dummy"));
        assertThat("future must wait for all subFutures", all.isDone(), Matchers.is(false));

        f2.complete(2);
        expectedException.expectCause(Matchers.instanceOf(IllegalStateException.class));
        all.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testAllAsListResultContainsListOfResults() throws Exception {
        CompletableFuture<Integer> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();
        CompletableFuture<List<Integer>> all = CompletableFutures.allAsList(Arrays.asList(f1, f2));

        f1.complete(10);
        f2.complete(20);

        assertThat(all.get(10, TimeUnit.SECONDS), Matchers.contains(10, 20));
    }
}
