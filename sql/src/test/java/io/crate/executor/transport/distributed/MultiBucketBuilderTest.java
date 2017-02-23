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

package io.crate.executor.transport.distributed;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row1;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MultiBucketBuilderTest {

    private List<MultiBucketBuilder> builders = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        builders.add(new ModuloBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 1, 0));
        builders.add(new BroadcastingBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 1));
    }

    @Test
    public void testBucketIsEmptyAfterSecondBuildBucket() throws Exception {
        Bucket[] buckets = new Bucket[1];
        for (MultiBucketBuilder builder : builders) {
            builder.add(new Row1(42));

            builder.build(buckets);
            assertThat(buckets[0].size(), is(1));

            builder.build(buckets);
            assertThat(buckets[0].size(), is(0));
        }
    }

    @Test
    public void testSizeIsResetOnBuildBuckets() throws Exception {
        Bucket[] buckets = new Bucket[1];

        for (MultiBucketBuilder builder : builders) {
            builder.add(new Row1(42));
            builder.add(new Row1(42));
            assertThat(builder.size(), is(2));

            builder.build(buckets);
            assertThat(buckets[0].size(), is(2));
            assertThat(builder.size(), is(0));
        }
    }
}
