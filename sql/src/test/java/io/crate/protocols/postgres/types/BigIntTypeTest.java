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

package io.crate.protocols.postgres.types;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class BigIntTypeTest extends BasePGTypeTest<Long> {

    public BigIntTypeTest() {
        super(BigIntType.INSTANCE);
    }

    @Test
    public void testWriteValue() throws Exception {
        assertBytesWritten(Long.MIN_VALUE, new byte[]{0, 0, 0, 8, -128, 0, 0, 0, 0, 0, 0, 0});
    }

    @Test
    public void testReadValueBinary() throws Exception {
        assertBytesReadBinary(new byte[]{127, -1, -1, -1, -1, -1, -1, -1}, Long.MAX_VALUE);
    }

    @Test
    public void testReadValueText() throws Exception {
        byte[] bytesToRead = String.valueOf(Long.MAX_VALUE).getBytes(StandardCharsets.UTF_8);
        assertBytesReadText(bytesToRead, Long.MAX_VALUE, bytesToRead.length);
    }
}
