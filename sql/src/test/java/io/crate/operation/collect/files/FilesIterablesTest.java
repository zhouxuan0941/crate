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

package io.crate.operation.collect.files;

import com.google.common.collect.Iterators;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class FilesIterablesTest {

    @Test
    public void testSqlFeatureIterable() throws Exception {
        SqlFeaturesIterable featuresIterable = new SqlFeaturesIterable();
        Iterator iterator = featuresIterable.iterator();
        assertTrue(iterator.hasNext());
        SqlFeatureContext context = (SqlFeatureContext) iterator.next();
        assertEquals("B011", context.featureId);
        assertEquals("Embedded Ada", context.featureName);
        assertEquals("", context.subFeatureId);
        assertEquals("", context.subFeatureName);
        assertEquals(false, context.isSupported);
        assertNull(context.isVerifiedBy);
        assertNull(context.comments);
        assertEquals(671L, Iterators.size(iterator));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSummitsIterable() throws Exception {
        SummitsIterable summitsIterable = new SummitsIterable();
        Iterator iterator = summitsIterable.iterator();
        assertTrue(iterator.hasNext());
        assertThat(Iterators.size(iterator), is(1605));
        assertFalse(iterator.hasNext());
    }
}
