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

package io.crate.analyze;

import io.crate.analyze.symbol.Symbol;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.util.Consumer;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QuerySpecTest {

    SQLExecutor e = SQLExecutor.builder(new NoopClusterService()).enableDefaultTables().build();

    @Test
    public void testVisitSymbolVisitsAllSymbols() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze(
            "select " +
            "       x," +           // 1
            "       count(*) " +    // 2
            "from t1 " +
            "where x = 2 " +        // 3 (function counts as 1 symbol)
            "group by 1 " +         // 4
            "having count(*) = 2 " +// 5
            "order by 2 " +         // 6
            "limit 1 " +            // 7
            "offset 1");            // 8
        final AtomicInteger numSymbols = new AtomicInteger(0);
        stmt.relation().querySpec().visitSymbols(new Consumer<Symbol>() {
            @Override
            public void accept(Symbol symbol) {
                numSymbols.incrementAndGet();
            }
        });
        assertThat(numSymbols.get(), is(8));
    }
}
