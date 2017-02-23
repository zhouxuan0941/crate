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

package io.crate.executor;

import com.google.common.base.Joiner;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BytesRefUtils;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.core.Is.is;

public class BytesRefUtilsTest extends CrateUnitTest {

    final static Joiner commaJoiner = Joiner.on(", ");

    @Test
    public void testEnsureStringTypesAreStringsSetString() throws Exception {
        DataType[] dataTypes = new DataType[]{new SetType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        Set<BytesRef> refs = new HashSet<>(
            Arrays.asList(new BytesRef("foo"), new BytesRef("bar")));

        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);
        assertThat((String[]) rows[0][0], Matchers.arrayContainingInAnyOrder("foo", "bar"));
    }

    @Test
    public void testEnsureStringTypesAreStringsArrayString() throws Exception {
        DataType[] dataTypes = new DataType[]{new ArrayType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        BytesRef[] refs = new BytesRef[]{new BytesRef("foo"), new BytesRef("bar")};

        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);
        assertThat(commaJoiner.join((String[]) rows[0][0]), is("foo, bar"));
    }

    @Test
    public void testConvertSetWithNullValues() throws Exception {
        DataType[] dataTypes = new DataType[]{new SetType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        Set<BytesRef> refs = new HashSet<>(
            Arrays.asList(new BytesRef("foo"), null));
        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);

        assertThat((String[]) rows[0][0], Matchers.arrayContainingInAnyOrder("foo", null));
    }
}
