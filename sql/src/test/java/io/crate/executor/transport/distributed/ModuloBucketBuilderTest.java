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
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ModuloBucketBuilderTest extends CrateUnitTest {

    @Test
    public void testRowsAreDistributedByModulo() throws Exception {
        final ModuloBucketBuilder builder = new ModuloBucketBuilder(new Streamer[]{DataTypes.INTEGER.streamer()}, 2, 0);

        builder.add(new Row1(1));
        builder.add(new Row1(2));
        builder.add(new Row1(3));
        builder.add(new Row1(4));

        Bucket[] buckets = new Bucket[2];
        builder.build(buckets);

        final Bucket rowsD1 = buckets[0];
        assertThat(rowsD1.size(), is(2));
        assertThat(TestingHelpers.printedTable(rowsD1), is("2\n4\n"));

        final Bucket rowsD2 = buckets[1];
        assertThat(rowsD2.size(), is(2));
        assertThat(TestingHelpers.printedTable(rowsD2), is("1\n3\n"));
    }
}
