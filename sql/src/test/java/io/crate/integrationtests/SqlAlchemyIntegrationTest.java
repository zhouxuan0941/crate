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

package io.crate.integrationtests;

import io.crate.testing.UseJdbc;
import org.junit.Test;

@UseJdbc
public class SqlAlchemyIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testSqlAlchemyGeneratedCountWithStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*')).filter(Test.name == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        refresh();

        execute(
            "SELECT count(?) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[]{"*", "foo"}
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(Test.col1).filter(Test.col2 == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        refresh();

        execute(
            "SELECT count(test.col1) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[]{"foo"}
        );
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithCountStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*'), Test.col2).group_by(Test.col2).order_by(desc(func.count('*'))).all()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        refresh();

        execute(
            "SELECT count(?) AS count_1, test.col2 AS test_col2 FROM test " +
            "GROUP BY test.col2 order by count_1 desc",
            new Object[]{"*"}
        );

        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testSqlAlchemyGroupByWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count(Test.col1), Test.col2).group_by(Test.col2).order_by(desc(func.count(Test.col1))).all()


        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        refresh();

        execute(
            "SELECT count(test.col1) AS count_1, test.col2 AS test_col2 FROM test " +
            "GROUP BY test.col2 order by count_1 desc"
        );

        assertEquals(2L, response.rows()[0][0]);
    }
}
