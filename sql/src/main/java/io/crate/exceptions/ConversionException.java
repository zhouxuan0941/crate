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

package io.crate.exceptions;

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;

import java.util.Locale;

public class ConversionException extends IllegalArgumentException {

    private final static String ERROR_MESSAGE = "Cannot cast %s to type %s";

    public ConversionException(Symbol symbol, DataType targetType) {
        super(generateMessage(symbol, targetType));
    }

    public ConversionException(Object value, DataType type) {
        super(generateMessage(value, type));
    }

    private static String generateMessage(Symbol value, DataType type) {
        return String.format(Locale.ENGLISH, ERROR_MESSAGE,
            SymbolPrinter.INSTANCE.printSimple(value), type.toString());
    }
    private static String generateMessage(Object value, DataType type) {
        if (value instanceof Symbol) {
            return generateMessage((Symbol) value, type);
        } else if (value instanceof BytesRef) {
            return String.format(Locale.ENGLISH, ERROR_MESSAGE,
                String.format(Locale.ENGLISH, "'%s'", ((BytesRef) value).utf8ToString()), type.toString());
        } else if (value instanceof String) {
            return String.format(Locale.ENGLISH, ERROR_MESSAGE,
                String.format(Locale.ENGLISH, "'%s'", value), type.toString());
        }
        return String.format(Locale.ENGLISH, ERROR_MESSAGE, value.toString(), type.toString());
    }
}
