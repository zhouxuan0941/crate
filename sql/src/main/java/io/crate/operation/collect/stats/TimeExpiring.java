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

package io.crate.operation.collect.stats;

import com.google.common.annotations.VisibleForTesting;
import io.crate.operation.reference.sys.job.ContextLog;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class TimeExpiring {

    /**
     * clean interval in milliseconds
     */
    private static final long DEFAULT_QUEUE_CLEAN_INTERVAL = 5000L;

    private static TimeExpiring INSTANCE = new TimeExpiring(DEFAULT_QUEUE_CLEAN_INTERVAL, 0L);
    private final long interval;
    private final long delay;

    TimeExpiring(long interval, long delay) {
        this.interval = interval;
        this.delay = delay;
    }

    public TimeExpiring(long interval) {
        this(interval, 0L);
    }

    public static TimeExpiring instance() {
        return INSTANCE;
    }

    public ScheduledFuture<?> registerTruncateTask(Queue<? extends ContextLog> q,
                                                   ScheduledExecutorService scheduler,
                                                   TimeValue expiration) {
        return scheduler.scheduleWithFixedDelay(
            () -> removeExpiredLogs(q, System.currentTimeMillis(), expiration.getMillis()),
            delay, interval, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    static <T extends ContextLog> void removeExpiredLogs(Queue<T> q, long currentTimeMillis, long expirationTime) {
        long expired = currentTimeMillis - expirationTime;
        for (T t : q) {
            if (t.ended() < expired) {
                q.remove();
            } else {
                break;
            }
        }
    }
}
