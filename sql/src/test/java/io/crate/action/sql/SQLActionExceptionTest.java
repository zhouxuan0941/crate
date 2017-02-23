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

package io.crate.action.sql;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SQLActionExceptionTest extends CrateUnitTest {

    @Test
    public void testFromSerializationWrapper() throws Exception {
        try {
            throw new SQLActionException("hello", 7, RestStatus.BAD_GATEWAY);
        } catch (SQLActionException cause) {
            NotSerializableExceptionWrapper wrapper = new NotSerializableExceptionWrapper(cause);
            SQLActionException unwrapped = SQLActionException.fromSerializationWrapper(wrapper);
            assertThat(unwrapped, instanceOf(SQLActionException.class));
            assertThat(unwrapped.getMessage(), equalTo(cause.getMessage()));
            assertThat(unwrapped.status(), equalTo(cause.status()));
            assertThat(unwrapped.errorCode(), equalTo(cause.errorCode()));
            assertNotNull(unwrapped.getStackTrace());
            assertThat(unwrapped.getStackTrace(), equalTo(cause.getStackTrace()));
        }
    }
}
