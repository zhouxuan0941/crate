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

package io.crate.testing;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RowGenerator {

    /**
     * Generate a range of rows.
     * Both increasing (e.g. 0 -> 10) and decreasing ranges (10 -> 0) are supported.
     *
     * @param from start (inclusive)
     * @param to   end (exclusive)
     */
    public static Iterable<Row> range(final long from, final long to) {
        return new Iterable<Row>() {

            @Override
            public Iterator<Row> iterator() {
                return new Iterator<Row>() {

                    private Object[] columns = new Object[1];
                    private RowN sharedRow = new RowN(columns);
                    private long i = from;
                    private long step = from < to ? 1 : -1;

                    @Override
                    public boolean hasNext() {
                        return step >= 0 ? i < to : i > to;
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("Iterator exhausted");
                        }
                        columns[0] = i;
                        i += step;
                        return sharedRow;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Remove not supported");
                    }
                };
            }
        };
    }

    public static List<Row> singleColRows(Object... rows) {
        List<Row> result = new ArrayList<>(rows.length);
        for (Object row : rows) {
            result.add(new Row1(row));
        }
        return result;
    }
}
