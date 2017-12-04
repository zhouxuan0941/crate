/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.where;

import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

public class EqualsExtractorTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();
    }

    private Symbol asSymbol(String expression) {
        return e.asSymbol(Collections.singletonMap(T3.T1, T3.TR_1), expression);
    }

    private List<EqualsExtractor.Match> extractMatches(String expression, String... columnToFind) {
        return EqualsExtractor.extractMatches(
            Stream.of(columnToFind).map(ColumnIdent::new).collect(Collectors.toList()),
            asSymbol(expression)
        );
    }

    @Test
    public void testSingleColumnMatch() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ?", "x");
        assertThat(
            matches.stream().map(EqualsExtractor.Match::toString).collect(Collectors.toList()),
            Matchers.contains(is("(x = $1)")));
    }

    @Test
    public void testSingleColumnMatchWithScalarSubquery() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = (select x from t1)", "x");
        assertThat(
            matches.stream().map(EqualsExtractor.Match::toString).collect(Collectors.toList()),
            Matchers.contains(is("(x = SelectSymbol{integer_table})")));
    }

    @Test
    public void testColumnEqColumn() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = a::integer", "x");
        assertThat(matches, Matchers.emptyIterable());
    }

    @Test
    public void testFindSingleColumnWithForeignColumn() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ? and a = 10", "x");
        assertThat(matches, Matchers.emptyIterable());
    }

    @Test
    public void testTwoColumnMatch() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ? and a = ?", "x", "a");
        List<String> result= matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(is("(x = $1), (a = $2)")));
    }

    @Test
    public void testSingleColumnMultipleOrMatch() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ? or x = ? or x = ?", "x");
        List<String> result = matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(
                is("(x = $1)"),
                is("(x = $2)"),
                is("(x = $3)")
            )
        );
    }

    @Test
    public void testTwoColumnsWithOrMatch() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("(x = ? and a = ?) or (x = ? and a = ?)", "x", "a");
        List<String> result= matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(
                is("(x = $1), (a = $2)"),
                is("(x = $3), (a = $4)")
            ));
    }

    @Test
    public void testWhereXEqAnyArrayLiteral() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ANY([1, 2, 3])", "x");
        List<String> result= matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(
                is("(x = ANY([1, 2, 3]))")
            ));
    }

    @Test
    public void testWhereXEqAnyArrayParam() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches("x = ANY(?)", "x");
        List<String> result= matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(
                is("(x = ANY($1))")
            ));
    }

    @Test
    public void testEqAnyWithMultipleColumnsAndOr() throws Exception {
        List<EqualsExtractor.Match> matches = extractMatches(
            "(x = ANY(?) and a = ANY(?)) or (x = ANY(?) and a = ANY(?))", "x", "a");
        List<String> result= matches.stream()
            .map(EqualsExtractor.Match::toString)
            .collect(Collectors.toList());
        assertThat(
            result,
            Matchers.contains(
                is("(x = ANY($1)), (a = ANY($2))"),
                is("(x = ANY($3)), (a = ANY($4))")
            ));
    }
}
