/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.Row;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;


public class ParameterContext implements Function<ParameterExpression, Literal> {

    public static final ParameterContext EMPTY = new ParameterContext(Row.EMPTY, Collections.<Row>emptyList());

    private final Row parameters;
    private final List<Row> bulkParameters;

    private int currentIdx = 0;
    private ParamTypeHints typeHints = null;


    public ParameterContext(Row parameters, List<Row> bulkParameters) {
        this.parameters = parameters;
        if (bulkParameters.size() > 0) {
            validateBulkParams(bulkParameters);
        }
        this.bulkParameters = bulkParameters;
    }

    private void validateBulkParams(List<Row> bulkParams) {
        int length = bulkParams.get(0).size();
        for (Row bulkParam : bulkParams) {
            if (bulkParam.size() != length) {
                throw new IllegalArgumentException("mixed number of arguments inside bulk arguments");
            }
        }
    }

    static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
        DataType guessedType = DataTypes.guessType(value);
        if (guessedType == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Got an argument \"%s\" that couldn't be recognized", value));
        }
        return guessedType;
    }

    public boolean hasBulkParams() {
        return bulkParameters.size() > 0;
    }

    public int numBulkParams() {
        return bulkParameters.size();
    }

    public void setBulkIdx(int i) {
        this.currentIdx = i;
    }

    public Row parameters() {
        if (hasBulkParams()) {
            return bulkParameters.get(currentIdx);
        }
        return parameters;
    }


    public ParamTypeHints typeHints() {
        if (typeHints == null) {
            List<DataType> types = new ArrayList<>(parameters.size());
            for (int i = 0; i < parameters.size(); i++) {
                types.add(guessTypeSafe(parameters.get(i)));
            }
            typeHints = new ParamTypeHints(types);
        }
        return typeHints;
    }

    @Override
    public Literal apply(ParameterExpression expression) {
        return Parameters.convert(parameters(), expression);
    }
}
